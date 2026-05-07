#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Optimized Mutual Fund Data Extractor - Tableau Hyper Output
Fetches NAV data via mftool with concurrent processing and persistent caching.
"""

import os
import json
import pickle
import time
import threading
import warnings
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache          # FIX: was used but never imported
from pathlib import Path

import pandas as pd
from mftool import Mftool

from tableauhyperapi import (
    HyperProcess, Telemetry, Connection, CreateMode,
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition,
    Inserter, TableName, HyperException,
)

warnings.filterwarnings("ignore")


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _fmt_seconds(seconds: float) -> str:
    """Human-readable elapsed / ETA string."""
    if seconds < 60:
        return f"{int(seconds)}s"
    if seconds < 3600:
        m, s = divmod(int(seconds), 60)
        return f"{m}m {s}s"
    h, rem = divmod(int(seconds), 3600)
    m = rem // 60
    return f"{h}h {m}m"


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class OptimizedMutualFundExtractor:
    """
    Fetch mutual fund NAV data from AMFI via mftool, cache it persistently,
    and export to a Tableau Hyper file.

    Persistent cache strategy
    -------------------------
    Historical NAV values never change, so the cache only accumulates — it is
    never expired or cleared.  Two pickle files are maintained:
        • nav_data_cache.pkl        – {scheme_code: {date_str: nav_float}}
        • scheme_metadata_cache.pkl – {scheme_code: {details, processed_metadata}}
    A lightweight cache_metadata.json tracks the last-updated timestamp.
    """

    # ------------------------------------------------------------------
    # Construction & initialisation
    # ------------------------------------------------------------------

    def __init__(self, max_workers: int = 10, api_delay: float = 0.1,
                 cache_dir: str = "nav_cache"):
        self.mf = Mftool()
        self.max_workers = max_workers
        self.api_delay = api_delay

        self.rate_limiter = threading.Semaphore(max_workers)
        self.results_lock = threading.Lock()
        self.cache_lock = threading.Lock()

        # Paths
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(self.script_dir)

        # FIX: use a single canonical output directory (was split across two
        # different paths in the original — hyper_files/ vs output/).
        self.output_dir = Path(project_root) / "output"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        cache_root = Path(project_root) / "data"
        cache_root.mkdir(parents=True, exist_ok=True)
        self.cache_dir = cache_root

        self.nav_cache_file      = self.cache_dir / "nav_data_cache.pkl"
        self.cache_metadata_file = self.cache_dir / "cache_metadata.json"
        self.metadata_cache_file = self.cache_dir / "scheme_metadata_cache.pkl"

        self.nav_cache      = self._load_nav_cache()
        self.metadata_cache = self._load_metadata_cache()
        self._reset_cache_stats()

    # ------------------------------------------------------------------
    # Cache stats helpers
    # ------------------------------------------------------------------

    def _reset_cache_stats(self):
        self.cache_stats = {
            "hits": 0, "misses": 0, "new_data": 0,
            "metadata_hits": 0, "metadata_misses": 0,
        }

    def _increment_cache_stat(self, key: str, increment: int = 1):
        self.cache_stats.setdefault(key, 0)
        self.cache_stats[key] += increment

    def _get_cache_stat(self, key: str, default: int = 0) -> int:
        return self.cache_stats.get(key, default)

    def _ensure_cache_stats_initialized(self):
        for key in ("hits", "misses", "new_data", "metadata_hits", "metadata_misses"):
            self.cache_stats.setdefault(key, 0)

    # ------------------------------------------------------------------
    # Date helpers
    # ------------------------------------------------------------------

    def _get_default_date_range(self):
        today = datetime.now()
        return datetime(today.year, 1, 1), today

    def _parse_date_input(self, date_input):
        if date_input is None:
            return None
        if isinstance(date_input, datetime):
            return date_input
        if isinstance(date_input, str):
            if len(date_input) == 10 and "-" in date_input:
                return datetime.strptime(date_input, "%Y-%m-%d")
            if len(date_input) == 4 and date_input.isdigit():
                return datetime(int(date_input), 1, 1)
            return pd.to_datetime(date_input).to_pydatetime()
        raise ValueError(f"Invalid date input type: {type(date_input)}")

    def _validate_date_range(self, start_date, end_date):
        if start_date and end_date and start_date > end_date:
            raise ValueError(
                f"Start date ({start_date.date()}) cannot be after end date ({end_date.date()})"
            )
        today = datetime.now().date()
        if end_date and end_date.date() > today:
            print(f"Warning: End date adjusted to {today} (cannot fetch future data)")
            end_date = datetime.combine(today, datetime.min.time())
        return start_date, end_date

    # ------------------------------------------------------------------
    # Persistent cache — load / save
    # ------------------------------------------------------------------

    def _load_pickle(self, path: Path, label: str):
        try:
            if path.exists():
                with open(path, "rb") as f:
                    data = pickle.load(f)
                print(f"Loaded {label}: {len(data)} entries")
                return data
        except Exception as e:
            print(f"Error loading {label}: {e}. Starting fresh.")
        return {}

    def _load_nav_cache(self):
        cache = self._load_pickle(self.nav_cache_file, "NAV cache")
        if cache and self.cache_metadata_file.exists():
            try:
                with open(self.cache_metadata_file) as f:
                    meta = json.load(f)
                print(f"  last updated: {meta.get('last_updated', 'Unknown')}")
            except Exception:
                pass
        return cache

    def _load_metadata_cache(self):
        return self._load_pickle(self.metadata_cache_file, "metadata cache")

    def _save_nav_cache(self):
        try:
            with self.cache_lock:
                with open(self.nav_cache_file, "wb") as f:
                    pickle.dump(self.nav_cache, f)

                total_records = sum(len(v) for v in self.nav_cache.values())
                meta = {
                    "last_updated": datetime.now().isoformat(),
                    "total_schemes": len(self.nav_cache),
                    "total_records": total_records,
                    "cache_policy": "persistent_accumulative",
                }
                with open(self.cache_metadata_file, "w") as f:
                    json.dump(meta, f, indent=2)

                print(f"Cache saved: {meta['total_schemes']} schemes, {total_records} records")
        except Exception as e:
            print(f"Warning: Error saving NAV cache: {e}")

    def _save_metadata_cache(self):
        try:
            with self.cache_lock:
                with open(self.metadata_cache_file, "wb") as f:
                    pickle.dump(self.metadata_cache, f)
                print(f"Metadata cache saved: {len(self.metadata_cache)} schemes")
        except Exception as e:
            print(f"Warning: Error saving metadata cache: {e}")

    # ------------------------------------------------------------------
    # Cache read / write
    # ------------------------------------------------------------------

    def _get_cached_nav_data(self, scheme_code, start_date, end_date):
        """
        Returns (cached_records, missing_date_range).
        missing_date_range is None when the cache is fresh enough to use as-is.
        """
        scheme_cache = self.nav_cache.get(scheme_code)
        if not scheme_cache:
            self._increment_cache_stat("misses")
            return [], (start_date, end_date)

        start_str = start_date.strftime("%Y-%m-%d")
        end_str   = end_date.strftime("%Y-%m-%d")

        cached_records = []
        for date_str, nav_value in scheme_cache.items():
            if start_str <= date_str <= end_str:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                cached_records.append({
                    "Scheme_Code": scheme_code,
                    "Date":        date_str,
                    "NAV":         nav_value,
                    "Year":        date_obj.year,
                    "Month":       date_obj.month,
                    "Day":         date_obj.day,
                    "Weekday":     date_obj.strftime("%A"),
                })

        if not cached_records:
            self._increment_cache_stat("misses")
            return [], (start_date, end_date)

        self._increment_cache_stat("hits")
        cached_records.sort(key=lambda r: r["Date"])

        latest_date = datetime.strptime(cached_records[-1]["Date"], "%Y-%m-%d")
        days_old    = (datetime.now() - latest_date).days

        # PERF: compute expected-date set once (was recomputed per-call in original)
        expected_dates  = set(
            d.strftime("%Y-%m-%d")
            for d in pd.date_range(start=start_date, end=end_date, freq="D")
        )
        cached_date_set = {r["Date"] for r in cached_records}
        missing_count   = len(expected_dates - cached_date_set)

        if days_old <= 3 and missing_count < 10:
            return cached_records, None     # cache is complete enough

        return cached_records, (start_date, end_date)

    def _get_cached_metadata(self, scheme_code):
        if scheme_code in self.metadata_cache:
            self._increment_cache_stat("metadata_hits")
            return self.metadata_cache[scheme_code]
        self._increment_cache_stat("metadata_misses")
        return None

    def _update_nav_cache(self, scheme_code, nav_records):
        if not nav_records:
            return
        with self.cache_lock:
            bucket = self.nav_cache.setdefault(scheme_code, {})
            new_count = 0
            for rec in nav_records:
                if rec["Date"] not in bucket:
                    bucket[rec["Date"]] = rec["NAV"]
                    new_count += 1
            if new_count:
                self._increment_cache_stat("new_data", new_count)

    def _update_metadata_cache(self, scheme_code, scheme_details, nav_quote, processed_metadata):
        with self.cache_lock:
            self.metadata_cache[scheme_code] = {
                "scheme_details":     scheme_details,
                "nav_quote":          nav_quote,
                "processed_metadata": processed_metadata,
                "cached_at":          datetime.now().isoformat(),
            }

    # ------------------------------------------------------------------
    # Rate-limited API wrappers
    # FIX: removed duplicate lru_cache wrappers — the persistent metadata
    # cache already deduplicates these calls; the in-process lru_cache added
    # memory overhead without benefit.
    # ------------------------------------------------------------------

    def _rate_limited_api_call(self, func, *args, **kwargs):
        with self.rate_limiter:
            try:
                result = func(*args, **kwargs)
                time.sleep(self.api_delay)
                return result
            except Exception:
                return None

    # ------------------------------------------------------------------
    # Filter normalisation
    # ------------------------------------------------------------------

    def _normalize_fund_house_filter(self, fund_house_filter):
        if fund_house_filter is None:
            return None
        if isinstance(fund_house_filter, str):
            return None if fund_house_filter.lower() in ("all", "none") else [fund_house_filter]
        return [str(x) for x in fund_house_filter]

    def _normalize_category_filter(self, category_filter):
        if category_filter is None:
            return None
        if isinstance(category_filter, str):
            if category_filter.lower() in ("all", "none"):
                return None
            return [c.strip() for c in category_filter.split(",")]
        return [str(x) for x in category_filter]

    # ------------------------------------------------------------------
    # ETA
    # ------------------------------------------------------------------

    def _calculate_eta(self, current_index, total_items, start_time) -> str:
        if current_index == 0:
            return "Calculating..."
        elapsed  = time.time() - start_time
        avg      = elapsed / current_index
        remaining = (total_items - current_index) * avg
        return _fmt_seconds(remaining)

    # ------------------------------------------------------------------
    # Scheme metadata
    # ------------------------------------------------------------------

    _CATEGORY_MAP = {
        "Equity":    ("equity", "growth", "large cap", "mid cap", "small cap"),
        "Debt":      ("debt", "income", "bond", "gilt", "corporate"),
        "Hybrid":    ("hybrid", "balanced", "aggressive", "conservative"),
        "Liquid":    ("liquid", "money market", "overnight"),
        "Index/ETF": ("index", "etf"),
    }

    def _classify_category(self, raw_category: str) -> str:
        lower = raw_category.lower()
        for label, keywords in self._CATEGORY_MAP.items():
            if any(kw in lower for kw in keywords):
                return label
        return "Others"

    def _process_single_scheme_metadata(self, scheme_info):
        code, name = scheme_info
        try:
            cached = self._get_cached_metadata(code)
            if cached:
                return cached["processed_metadata"]

            scheme_data = self._rate_limited_api_call(self.mf.get_scheme_details, code)
            if not scheme_data:
                return None

            nav_data    = self._rate_limited_api_call(self.mf.get_scheme_quote, code)
            current_nav = nav_data.get("nav") if nav_data else None

            fund_info = {
                "Scheme_Code":           code,
                "Scheme_Name":           name,
                "Fund_House":            scheme_data.get("fund_house", ""),
                "Scheme_Type":           scheme_data.get("scheme_type", ""),
                "Scheme_Category":       scheme_data.get("scheme_category", ""),
                "Scheme_Start_Date_Info":scheme_data.get("scheme_start_date", ""),
                "Current_NAV":           current_nav,
                "Last_Updated":          nav_data.get("last_updated", "") if nav_data else "",
                "Main_Category":         self._classify_category(scheme_data.get("scheme_category", "")),
            }

            self._update_metadata_cache(code, scheme_data, nav_data, fund_info)
            return fund_info

        except Exception as e:
            print(f"  Error processing scheme {code}: {e}")
            return None

    def get_mutual_fund_metadata(self, fund_house_filter=None, category_filter=None) -> pd.DataFrame:
        fund_houses = self._normalize_fund_house_filter(fund_house_filter)
        categories  = self._normalize_category_filter(category_filter)

        print(f"Fetching funds for: {', '.join(fund_houses) if fund_houses else 'ALL'}")
        print(f"Category filter:    {', '.join(categories) if categories else 'ALL'}")

        print("Fetching scheme codes...")
        all_schemes = self.mf.get_scheme_codes()
        if not all_schemes:
            print("Failed to fetch scheme codes.")
            return pd.DataFrame()

        print(f"Found {len(all_schemes)} total schemes")

        if fund_houses:
            filtered = {
                code: name for code, name in all_schemes.items()
                if any(fh.lower() in name.lower() for fh in fund_houses)
            }
        else:
            filtered = all_schemes

        total      = len(filtered)
        start_time = time.time()
        print(f"Processing {total} schemes with {self.max_workers} threads...")

        fund_data      = []
        completed      = 0
        cats_lower     = [c.lower() for c in categories] if categories else []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._process_single_scheme_metadata, (code, name)): (code, name)
                for code, name in filtered.items()
            }
            for future in as_completed(futures):
                completed += 1
                if completed % 50 == 0 or completed == total:
                    eta = self._calculate_eta(completed, total, start_time)
                    print(f"Progress: {completed}/{total} | ETA: {eta}")

                try:
                    result = future.result()
                    if result:
                        if cats_lower and not any(
                            c in result["Main_Category"].lower() for c in cats_lower
                        ):
                            continue
                        with self.results_lock:
                            fund_data.append(result)
                except Exception as e:
                    code, _ = futures[future]
                    print(f"  Error: scheme {code}: {e}")

        df = pd.DataFrame(fund_data)
        elapsed = time.time() - start_time
        print(f"\nFetched metadata for {len(df)} funds in {elapsed:.1f}s")
        return df

    # ------------------------------------------------------------------
    # NAV data
    # ------------------------------------------------------------------

    def _process_single_scheme_nav(self, scheme_code, start_date, end_date):
        cached_records, missing_range = self._get_cached_nav_data(scheme_code, start_date, end_date)

        if cached_records and missing_range is None:
            return cached_records

        api_records = []
        try:
            historical_data = self._rate_limited_api_call(
                self.mf.get_scheme_historical_nav, scheme_code, as_Dataframe=True
            )
            if historical_data is None:
                raw = self._rate_limited_api_call(self.mf.get_scheme_historical_nav, scheme_code)
                if isinstance(raw, dict):
                    historical_data = pd.DataFrame(raw)

            if historical_data is not None and not historical_data.empty:
                # Detect date / NAV columns
                if historical_data.index.name and "date" in historical_data.index.name.lower():
                    historical_data = historical_data.reset_index()

                date_col = next(
                    (c for c in historical_data.columns if "date" in c.lower()), None
                )
                nav_col  = next(
                    (c for c in historical_data.columns if "nav"  in c.lower()), None
                )

                if date_col and nav_col:
                    date_converted = False
                    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
                        try:
                            historical_data[date_col] = pd.to_datetime(
                                historical_data[date_col], format=fmt
                            )
                            date_converted = True
                            break
                        except Exception:
                            continue

                    if not date_converted:
                        try:
                            historical_data[date_col] = pd.to_datetime(
                                historical_data[date_col], infer_datetime_format=True
                            )
                            date_converted = True
                        except Exception:
                            return cached_records

                    if date_converted:
                        mask = (
                            (historical_data[date_col] >= start_date) &
                            (historical_data[date_col] <= end_date)
                        )
                        filtered = historical_data.loc[mask].sort_values(date_col)

                        for row in filtered.itertuples(index=False):   # PERF: itertuples > iterrows
                            try:
                                nav_val  = float(getattr(row, nav_col))
                                date_val = getattr(row, date_col)
                                api_records.append({
                                    "Scheme_Code": scheme_code,
                                    "Date":        date_val.strftime("%Y-%m-%d"),
                                    "NAV":         nav_val,
                                    "Year":        date_val.year,
                                    "Month":       date_val.month,
                                    "Day":         date_val.day,
                                    "Weekday":     date_val.strftime("%A"),
                                })
                            except (ValueError, TypeError):
                                continue

                        if api_records:
                            self._update_nav_cache(scheme_code, api_records)

        except Exception:
            pass  # fall through to return whatever we have

        # Merge cached + api, deduplicate, clip to range
        all_records = cached_records + api_records
        start_str   = start_date.strftime("%Y-%m-%d")
        end_str     = end_date.strftime("%Y-%m-%d")

        seen  = set()
        final = []
        for rec in sorted(all_records, key=lambda r: r["Date"]):
            if start_str <= rec["Date"] <= end_str and rec["Date"] not in seen:
                final.append(rec)
                seen.add(rec["Date"])
        return final

    def fetch_daily_nav_data(self, scheme_codes, start_date=None, end_date=None) -> pd.DataFrame:
        if start_date is None and end_date is None:
            start_date, end_date = self._get_default_date_range()
            print(f"Default date range (YTD): {start_date:%Y-%m-%d} to {end_date:%Y-%m-%d}")
        else:
            start_date = self._parse_date_input(start_date) if start_date else self._get_default_date_range()[0]
            end_date   = self._parse_date_input(end_date)   if end_date   else datetime.now()

        start_date, end_date = self._validate_date_range(start_date, end_date)
        print(f"\nFetching DAILY NAV: {start_date:%Y-%m-%d} → {end_date:%Y-%m-%d}")

        total      = len(scheme_codes)
        start_time = time.time()
        self._reset_cache_stats()

        all_nav_data = []
        completed    = 0
        batch_size   = min(50, self.max_workers * 3)

        for batch_start in range(0, total, batch_size):
            batch = scheme_codes[batch_start : batch_start + batch_size]
            batch_data = []

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(self._process_single_scheme_nav, code, start_date, end_date): code
                    for code in batch
                }
                for future in as_completed(futures):
                    completed += 1
                    if completed % 25 == 0 or completed == total:
                        hits     = self._get_cache_stat("hits")
                        misses   = self._get_cache_stat("misses")
                        hit_rate = hits / max(1, hits + misses) * 100
                        eta      = self._calculate_eta(completed, total, start_time)
                        print(f"NAV Progress: {completed}/{total} | ETA: {eta} | Cache: {hit_rate:.1f}%")

                    try:
                        records = future.result()
                        if records:
                            batch_data.extend(records)
                    except Exception as e:
                        print(f"  Error: scheme {futures[future]}: {e}")

            all_nav_data.extend(batch_data)
            del batch_data

            batch_num = batch_start // batch_size + 1
            print(f"Batch {batch_num} done. Total records: {len(all_nav_data)}")

            if batch_num % 5 == 0:
                print("Periodic cache save...")
                self._save_nav_cache()

        print("Saving final cache...")
        self._save_nav_cache()

        nav_df  = pd.DataFrame(all_nav_data)
        elapsed = time.time() - start_time
        print(f"\nFetched {len(nav_df)} daily NAV records in {elapsed:.1f}s")
        self.get_cache_stats()

        if not nav_df.empty:
            print(f"Date range in data: {nav_df['Date'].min()} → {nav_df['Date'].max()}")
            unique = nav_df["Scheme_Code"].nunique()
            print(f"Schemes with data:  {unique}  |  avg {len(nav_df)/max(1,unique):.0f} records/scheme")

        return nav_df

    # ------------------------------------------------------------------
    # Hyper export
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_float(value):
        if pd.isna(value) or value is None:
            return None
        try:
            f = float(value)
            return f if 0 < f < 100_000 else None
        except (ValueError, TypeError):
            return None

    def create_hyper_file(self, metadata_df: pd.DataFrame, nav_df: pd.DataFrame,
                          filename: str) -> bool:
        # FIX: unified output path (was inconsistently hyper_files/ in one place
        # and output/ in another)
        output_filepath = str(self.output_dir / filename)
        print(f"\nCreating Tableau Hyper file: {output_filepath}")

        try:
            with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                with Connection(
                    endpoint=hyper.endpoint,
                    database=output_filepath,
                    create_mode=CreateMode.CREATE_AND_REPLACE,
                ) as conn:

                    # ---- Schema definitions ----
                    metadata_table = TableDefinition(
                        table_name=TableName("Fund_Metadata"),
                        columns=[
                            TableDefinition.Column("Scheme_Code",            SqlType.text(), NOT_NULLABLE),
                            TableDefinition.Column("Scheme_Name",            SqlType.text(), NULLABLE),
                            TableDefinition.Column("Fund_House",             SqlType.text(), NULLABLE),
                            TableDefinition.Column("Scheme_Type",            SqlType.text(), NULLABLE),
                            TableDefinition.Column("Scheme_Category",        SqlType.text(), NULLABLE),
                            TableDefinition.Column("Scheme_Start_Date_Info", SqlType.text(), NULLABLE),
                            TableDefinition.Column("Current_NAV",            SqlType.text(), NULLABLE),
                            TableDefinition.Column("Last_Updated",           SqlType.text(), NULLABLE),
                            TableDefinition.Column("Main_Category",          SqlType.text(), NULLABLE),
                        ],
                    )
                    nav_table = TableDefinition(
                        table_name=TableName("Daily_NAV_Data"),
                        columns=[
                            TableDefinition.Column("Scheme_Code", SqlType.text(),   NOT_NULLABLE),
                            TableDefinition.Column("Date",        SqlType.date(),   NOT_NULLABLE),
                            TableDefinition.Column("NAV",         SqlType.double(), NULLABLE),
                            TableDefinition.Column("Year",        SqlType.int(),    NULLABLE),
                            TableDefinition.Column("Month",       SqlType.int(),    NULLABLE),
                            TableDefinition.Column("Day",         SqlType.int(),    NULLABLE),
                            TableDefinition.Column("Weekday",     SqlType.text(),   NULLABLE),
                        ],
                    )

                    conn.catalog.create_table(metadata_table)
                    conn.catalog.create_table(nav_table)
                    print("Tables created.")

                    # ---- Insert metadata ----
                    # PERF: pre-build row list; itertuples is ~5-10x faster than iterrows
                    if not metadata_df.empty:
                        print("Inserting metadata...")
                        text_cols = [
                            "Scheme_Code", "Scheme_Name", "Fund_House", "Scheme_Type",
                            "Scheme_Category", "Scheme_Start_Date_Info",
                            "Current_NAV", "Last_Updated", "Main_Category",
                        ]
                        rows = []
                        for t in metadata_df[text_cols].itertuples(index=False):
                            rows.append([
                                str(v) if (v is not None and not (isinstance(v, float) and pd.isna(v))) else None
                                for v in t
                            ])

                        batch_size = 5_000
                        with Inserter(conn, metadata_table) as ins:
                            for i in range(0, len(rows), batch_size):
                                for row in rows[i : i + batch_size]:
                                    ins.add_row(row)
                                print(f"  Metadata rows inserted: {min(i+batch_size, len(rows))}")
                            ins.execute()
                        print(f"Inserted {len(rows)} metadata records.")

                    # ---- Insert NAV data ----
                    if not nav_df.empty:
                        print("Inserting NAV data...")
                        batch_size = 50_000
                        with Inserter(conn, nav_table) as ins:
                            for i in range(0, len(nav_df), batch_size):
                                chunk = nav_df.iloc[i : i + batch_size]
                                for t in chunk.itertuples(index=False):
                                    try:
                                        ins.add_row([
                                            str(t.Scheme_Code),
                                            pd.to_datetime(t.Date).date(),
                                            self._safe_float(t.NAV),
                                            int(t.Year)    if pd.notna(t.Year)    else None,
                                            int(t.Month)   if pd.notna(t.Month)   else None,
                                            int(t.Day)     if pd.notna(t.Day)     else None,
                                            str(t.Weekday) if pd.notna(t.Weekday) else None,
                                        ])
                                    except Exception:
                                        continue
                                print(f"  NAV rows inserted: {min(i+batch_size, len(nav_df))}")
                            ins.execute()
                        print(f"Inserted {len(nav_df)} NAV records.")

            print(f"Hyper file created: {output_filepath}")
            return True

        except HyperException as ex:
            print(f"Hyper error: {ex}")
            return False
        except Exception as e:
            print(f"Error creating Hyper file: {e}")
            return False

    # ------------------------------------------------------------------
    # Cache diagnostics
    # ------------------------------------------------------------------

    def get_cache_stats(self):
        self._ensure_cache_stats_initialized()
        total_schemes  = len(self.nav_cache)
        total_records  = sum(len(v) for v in self.nav_cache.values())
        hits, misses   = self._get_cache_stat("hits"), self._get_cache_stat("misses")
        mhits, mmisses = self._get_cache_stat("metadata_hits"), self._get_cache_stat("metadata_misses")

        print("\n" + "="*50)
        print("NAV CACHE STATISTICS")
        print("="*50)
        print(f"Cached schemes:     {total_schemes}")
        print(f"Cached records:     {total_records}")
        print(f"Cache hits/misses:  {hits} / {misses}")
        print(f"New data fetched:   {self._get_cache_stat('new_data')}")
        if hits + misses:
            print(f"Hit rate:           {hits/(hits+misses)*100:.1f}%")
        print(f"\nMetadata cached:    {len(self.metadata_cache)} schemes")
        print(f"Metadata hits/miss: {mhits} / {mmisses}")
        if mhits + mmisses:
            print(f"Metadata hit rate:  {mhits/(mhits+mmisses)*100:.1f}%")
        if self.nav_cache_file.exists():
            size_mb = self.nav_cache_file.stat().st_size / 1_048_576
            print(f"\nCache file size: {size_mb:.2f} MB")

    def show_cache_growth_benefits(self):
        total_records = sum(len(v) for v in self.nav_cache.values())
        total_schemes = len(self.nav_cache)
        print("\n" + "="*60)
        print("PERSISTENT CACHE STATUS")
        print("="*60)
        print(f"  Schemes cached:   {total_schemes}")
        print(f"  NAV records:      {total_records:,}")
        print(f"  Metadata cached:  {len(self.metadata_cache)} schemes")
        if total_schemes:
            print(f"  Avg records/scheme: {total_records/total_schemes:.1f}")

    def check_cache_health(self):
        self._ensure_cache_stats_initialized()
        print("\n" + "="*50)
        print("CACHE HEALTH CHECK")
        print("="*50)
        total_records = sum(len(v) for v in self.nav_cache.values())
        total_schemes = len(self.nav_cache)

        if not total_schemes:
            print("Cache is empty.")
            return

        estimated = total_schemes * 365
        coverage  = min(total_records / estimated * 100, 100)
        print(f"NAV records:    {total_records:,}")
        print(f"Est. coverage:  {coverage:.1f}%")

        hits    = self._get_cache_stat("hits")
        misses  = self._get_cache_stat("misses")
        mhits   = self._get_cache_stat("metadata_hits")
        mmisses = self._get_cache_stat("metadata_misses")
        total   = hits + misses + mhits + mmisses
        if total:
            overall = (hits + mhits) / total * 100
            print(f"Overall efficiency: {overall:.1f}%")

        if self.cache_metadata_file.exists():
            try:
                with open(self.cache_metadata_file) as f:
                    meta = json.load(f)
                print(f"Last updated: {meta.get('last_updated','Unknown')}")
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def _cleanup_log_files(self):
        # PERF: use pathlib.Path.rglob — no need to re-import glob
        log_files = list(Path(".").rglob("*.log"))
        removed   = 0
        for p in log_files:
            try:
                p.unlink()
                removed += 1
            except Exception:
                continue
        if removed:
            print(f"Cleaned up {removed} log file(s)")

    # ------------------------------------------------------------------
    # Summary stats
    # ------------------------------------------------------------------

    def get_fund_summary_stats(self, metadata_df: pd.DataFrame):
        print("\n" + "="*60)
        print("FUND SUMMARY STATISTICS")
        print("="*60)
        print(f"Total funds: {len(metadata_df)}")

        if "Main_Category" in metadata_df.columns:
            print("\nBy category:")
            for cat, n in metadata_df["Main_Category"].value_counts().items():
                print(f"  • {cat}: {n}")

        if "Fund_House" in metadata_df.columns:
            print(f"\nFund houses: {metadata_df['Fund_House'].nunique()}")
            print("Top 10 by scheme count:")
            for fh, n in metadata_df["Fund_House"].value_counts().head(10).items():
                print(f"  • {fh}: {n}")

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def extract_fund_data(self,
                          fund_house_filter=None,
                          category_filter=None,
                          start_date=None,
                          end_date=None,
                          output_filename=None):
        """
        Full pipeline: metadata → NAV → Hyper file.

        Parameters
        ----------
        fund_house_filter : str | list | None
            E.g. "Quant" or ["Quant", "HDFC"] or None for all.
        category_filter : str | list | None
            E.g. "Equity" or ["Equity", "Debt"] or None for all.
        start_date : str | datetime | None
            'YYYY-MM-DD', 'YYYY', datetime, or None (→ Jan 1 of current year).
        end_date : str | datetime | None
            'YYYY-MM-DD', 'YYYY', datetime, or None (→ today).
        output_filename : str | None
            Custom .hyper filename, or None for auto-generation.
        """
        print("MUTUAL FUND DATA EXTRACTION — TABLEAU HYPER OUTPUT")
        print("=" * 70)
        print(f"Threads: {self.max_workers} | API delay: {self.api_delay}s")
        print(f"Cache:   {self.cache_dir}")
        print(f"Output:  {self.output_dir}")

        # Resolve dates for display & filename
        parsed_start = self._parse_date_input(start_date) if start_date else self._get_default_date_range()[0]
        parsed_end   = self._parse_date_input(end_date)   if end_date   else datetime.now()
        parsed_start, parsed_end = self._validate_date_range(parsed_start, parsed_end)
        print(f"Date range: {parsed_start:%Y-%m-%d} → {parsed_end:%Y-%m-%d}")

        # Auto-generate filename
        if output_filename is None:
            parts = []
            if fund_house_filter:
                fh = fund_house_filter if isinstance(fund_house_filter, list) else [fund_house_filter]
                parts.append("_".join(fh))
            else:
                parts.append("AllFunds")

            if category_filter:
                cf = category_filter if isinstance(category_filter, list) else [category_filter]
                parts.append("_".join(cf))

            date_tag = (
                f"{parsed_start.year}"
                if parsed_start.year == parsed_end.year
                   and parsed_start.month == 1 and parsed_start.day == 1
                else f"{parsed_start:%Y%m%d}_{parsed_end:%Y%m%d}"
            )
            parts.append(f"Daily_{date_tag}")
            output_filename = "_".join(parts) + ".hyper"
        elif not output_filename.endswith(".hyper"):
            output_filename += ".hyper"

        # Step 1 — metadata
        metadata_df = self.get_mutual_fund_metadata(fund_house_filter, category_filter)
        if metadata_df.empty:
            print("No funds matched the criteria. Exiting.")
            return

        self.get_fund_summary_stats(metadata_df)

        # Step 2 — NAV data
        scheme_codes = metadata_df["Scheme_Code"].tolist()
        nav_df = self.fetch_daily_nav_data(scheme_codes, start_date, end_date)
        if nav_df.empty:
            print("Warning: no NAV data fetched — Hyper file will contain metadata only.")
            nav_df = pd.DataFrame(
                columns=["Scheme_Code", "Date", "NAV", "Year", "Month", "Day", "Weekday"]
            )

        # Step 3 — export
        success = self.create_hyper_file(metadata_df, nav_df, output_filename)

        # Persist caches
        self._save_nav_cache()
        self._save_metadata_cache()
        self._cleanup_log_files()

        if success:
            print("\n" + "="*70)
            print("EXTRACTION COMPLETE")
            print("="*70)
            print(f"Output: {self.output_dir / output_filename}")
            print(f"Metadata records:  {len(metadata_df)}")
            print(f"Daily NAV records: {len(nav_df)}")

            hits   = self._get_cache_stat("hits")
            misses = self._get_cache_stat("misses")
            if hits + misses:
                print(f"\nCache hit rate: {hits/(hits+misses)*100:.1f}%  |  "
                      f"API calls saved: {hits}  |  "
                      f"New records cached: {self._get_cache_stat('new_data')}")

            if not nav_df.empty:
                unique = nav_df["Scheme_Code"].nunique()
                print(f"\nNAV date range: {nav_df['Date'].min()} → {nav_df['Date'].max()}")
                print(f"Schemes with data: {unique}  |  "
                      f"avg {len(nav_df)/max(1,unique):.0f} days/scheme")
        else:
            print("Extraction failed — check errors above.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    total_start = time.time()

    print("=" * 70)
    print("OPTIMIZED MUTUAL FUND DATA EXTRACTOR — TABLEAU HYPER")
    print("=" * 70)

    extractor = OptimizedMutualFundExtractor(
        max_workers=15,
        api_delay=0.5,
    )

    extractor.show_cache_growth_benefits()

    extractor.extract_fund_data(start_date="2010-01-01")

    extractor.get_cache_stats()
    extractor.check_cache_health()

    elapsed = time.time() - total_start
    print("\n" + "=" * 70)
    print("TOTAL EXECUTION TIME")
    print("=" * 70)
    print(f"Started:  {datetime.fromtimestamp(total_start):%Y-%m-%d %H:%M:%S}")
    print(f"Finished: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"Elapsed:  {_fmt_seconds(elapsed)}")