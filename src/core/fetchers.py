"""
Creator: @Saket
Email: anshusaketsingh@gmail.com
"""

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from tqdm import tqdm

from .utils import (
    get_default_date_range,
    parse_date_input,
    validate_date_range,
    normalize_filter,
    classify_category
)

class MetadataFetcher:
    def __init__(self, api, cache, max_workers: int = 10):
        self.api = api
        self.cache = cache
        self.max_workers = max_workers

    def _process_single_metadata(self, args):
        code, name = args
        try:
            scheme_data = self.api.get_scheme_details(code)
            if not scheme_data or not isinstance(scheme_data, dict):
                return None

            fund_house = scheme_data.get("fund_house", "Unknown")
            if not fund_house or fund_house.strip() == "":
                fund_house = name.split()[0] if name else "Unknown"

            fund_info = {
                "Scheme_Code":           code,
                "Scheme_Name":           name,
                "Fund_House":            fund_house,
                "Scheme_Type":           scheme_data.get("scheme_type", "Unknown"),
                "Scheme_Category":       scheme_data.get("scheme_category", "Unknown"),
                "Scheme_Start_Date_Info": scheme_data.get("scheme_start_date", ""),
                "Current_NAV":           None, 
                "Last_Updated":          datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Main_Category":         classify_category(scheme_data.get("scheme_category", "")),
            }
            self.cache.update_metadata_cache(code, scheme_data, {}, fund_info)
            return fund_info
        except Exception:
            return None

    def get_metadata(self, fund_house_filter=None, category_filter=None, progress_callback=None) -> pd.DataFrame:
        fund_houses = normalize_filter(fund_house_filter)
        categories = normalize_filter(category_filter)

        print("Fetching scheme codes...")
        all_schemes = self.api.get_scheme_codes()
        if not all_schemes:
            print("Failed to fetch scheme codes.")
            return pd.DataFrame()

        if fund_houses:
            filtered = {
                code: name for code, name in all_schemes.items()
                if any(fh.lower() in name.lower() for fh in fund_houses)
            }
        else:
            filtered = all_schemes

        fund_data = []
        to_fetch = {}
        cats_lower = [c.lower() for c in categories] if categories else []

        for code, name in filtered.items():
            hit = self.cache.get_cached_metadata(code)
            if hit and hit.get("processed_metadata"):
                result = hit["processed_metadata"]
                if not (cats_lower and not any(c in result.get("Main_Category", "").lower() for c in cats_lower)):
                    fund_data.append(result)
            else:
                to_fetch[code] = name

        print(f"Cache: {len(fund_data)} hits, {len(to_fetch)} to fetch via API")

        if to_fetch:
            print(f"Fetching {len(to_fetch)} missing metadata records...")
            with tqdm(total=len(to_fetch), desc="Metadata Progress", unit="scheme") as pbar:
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = {
                        executor.submit(self._process_single_metadata, (code, name)): (code, name)
                        for code, name in to_fetch.items()
                    }
                    completed = 0
                    for future in as_completed(futures):
                        res = future.result()
                        if res:
                            if not (cats_lower and not any(c in res.get("Main_Category", "").lower() for c in cats_lower)):
                                fund_data.append(res)
                        pbar.update(1)
                        completed += 1
                        if progress_callback:
                            progress_callback(completed, len(to_fetch), "Fetching Metadata")

        return pd.DataFrame(fund_data)


class NavFetcher:
    def __init__(self, api, cache, max_workers: int = 10):
        self.api = api
        self.cache = cache
        self.max_workers = max_workers

    def get_daily_nav(self, scheme_codes, start_date=None, end_date=None, progress_callback=None) -> pd.DataFrame:
        if start_date is None and end_date is None:
            start_date, end_date = get_default_date_range()
        else:
            start_date = parse_date_input(start_date) if start_date else get_default_date_range()[0]
            end_date   = parse_date_input(end_date)   if end_date   else datetime.now()

        start_date, end_date = validate_date_range(start_date, end_date)
        start_str = start_date.strftime("%Y-%m-%d")
        end_str   = end_date.strftime("%Y-%m-%d")
        scheme_codes = [str(c) for c in scheme_codes]
        
        # 1. Filter cache
        if not self.cache.nav_cache_df.empty:
            self.cache.nav_cache_df["Scheme_Code"] = self.cache.nav_cache_df["Scheme_Code"].astype(str)
            mask = (self.cache.nav_cache_df["Date"] >= start_str) & (self.cache.nav_cache_df["Date"] <= end_str)
            mask &= self.cache.nav_cache_df["Scheme_Code"].isin(scheme_codes)
            valid_cache = self.cache.nav_cache_df.loc[mask]
        else:
            valid_cache = pd.DataFrame(columns=["Scheme_Code", "Date", "NAV"])
            
        # 2. Identify schemes to fetch
        latest_dates = {}
        earliest_dates = {}
        if not self.cache.nav_cache_df.empty:
            # We must use the full cache to find the true boundaries, not the filtered valid_cache
            full_mask = self.cache.nav_cache_df["Scheme_Code"].isin(scheme_codes)
            full_cache_subset = self.cache.nav_cache_df.loc[full_mask]
            if not full_cache_subset.empty:
                latest_dates = full_cache_subset.groupby("Scheme_Code")["Date"].max().to_dict()
                earliest_dates = full_cache_subset.groupby("Scheme_Code")["Date"].min().to_dict()

        is_fresh = self.cache.is_cache_fresh(hours=360)  # 15 days
        schemes_to_fetch = []
        
        for code in scheme_codes:
            if code not in latest_dates:
                # If we just synced everything in the last 12 hours and this scheme 
                # is STILL completely missing from the cache, it means the API 
                # returned zero records for it (e.g. invalid code or completely closed).
                # Skip it to prevent infinitely hammering the API for dead schemes.
                if is_fresh:
                    continue
                    
                schemes_to_fetch.append(code)
                self.cache.increment_cache_stat("misses")
            else:
                last_date = latest_dates[code]
                first_date = earliest_dates[code]
                days_old = (datetime.now() - datetime.strptime(last_date, "%Y-%m-%d")).days
                
                # Fetch if the latest data is stale AND the global cache isn't marked as freshly updated
                needs_update = not is_fresh and days_old > 3
                
                # We only need history if the requested start date is older than both:
                # 1. The first date we have for this scheme
                # 2. The oldest date we have EVER synced across the entire cache
                oldest_synced = getattr(self.cache, "oldest_synced_date", "9999-12-31")
                needs_history = (start_str < first_date) and (start_str < oldest_synced)
                
                if needs_update or needs_history:
                    schemes_to_fetch.append(code)
                    self.cache.increment_cache_stat("misses")
                else:
                    self.cache.increment_cache_stat("hits")
                    
        if not schemes_to_fetch:
            print("Cache contains all requested historical data and is up to date. Skipping API fetch.")

        # 3. Fetch missing
        new_api_records = []
        def process_api(code):
            try:
                historical_data = self.api.get_scheme_historical_nav(code, as_Dataframe=True)
                if historical_data is None:
                    raw = self.api.get_scheme_historical_nav(code, as_Dataframe=False)
                    if isinstance(raw, dict):
                        historical_data = pd.DataFrame(raw)
                        
                if historical_data is not None and not historical_data.empty:
                    if historical_data.index.name and "date" in historical_data.index.name.lower():
                        historical_data = historical_data.reset_index()
                    date_col = next((c for c in historical_data.columns if "date" in c.lower()), None)
                    nav_col  = next((c for c in historical_data.columns if "nav"  in c.lower()), None)
                    
                    if date_col and nav_col:
                        historical_data[date_col] = pd.to_datetime(historical_data[date_col], format="mixed", dayfirst=True, errors="coerce")
                        historical_data = historical_data.dropna(subset=[date_col])
                        
                        records = []
                        for row in historical_data.itertuples(index=False):
                            try:
                                records.append({
                                    "Scheme_Code": str(code),
                                    "Date": getattr(row, date_col).strftime("%Y-%m-%d"),
                                    "NAV": float(getattr(row, nav_col))
                                })
                            except (ValueError, TypeError):
                                continue
                        return records
            except Exception:
                pass
            return []

        if schemes_to_fetch:
            print(f"Fetching NAV data via API for {len(schemes_to_fetch)} schemes...")
            with tqdm(total=len(schemes_to_fetch), desc="NAV Progress", unit="scheme") as pbar:
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = {executor.submit(process_api, code): code for code in schemes_to_fetch}
                    completed = 0
                    for future in as_completed(futures):
                        res = future.result()
                        if res:
                            with self.cache.cache_lock:
                                new_api_records.extend(res)
                        pbar.update(1)
                        completed += 1
                        if progress_callback:
                            progress_callback(completed, len(schemes_to_fetch), "Fetching NAV")

        if new_api_records:
            self.cache.increment_cache_stat("new_data", len(new_api_records))
            new_df = pd.DataFrame(new_api_records)
            self.cache.nav_cache_df = pd.concat([self.cache.nav_cache_df, new_df], ignore_index=True)
            
            # Re-filter after update
            mask = (self.cache.nav_cache_df["Date"] >= start_str) & (self.cache.nav_cache_df["Date"] <= end_str)
            mask &= self.cache.nav_cache_df["Scheme_Code"].isin(scheme_codes)
            valid_cache = self.cache.nav_cache_df.loc[mask]

        # Always update the globally tracked oldest_synced_date so we don't repeatedly
        # fetch for history that simply doesn't exist (e.g. fund was incepted later)
        oldest_synced = getattr(self.cache, "oldest_synced_date", "9999-12-31")
        if start_str < oldest_synced:
            self.cache.oldest_synced_date = start_str
            
        self.cache.save_nav_cache()
        self.cache.save_metadata_cache()
        return valid_cache
