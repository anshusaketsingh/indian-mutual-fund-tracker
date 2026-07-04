"""
Creator: @Saket
Email: anshusaketsingh@gmail.com
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Indian Stock Market Holiday Scraper
@author: anshusaketsingh

Architecture (no pickle cache needed):
  • Holiday *dates* → exchange_calendars (XBOM) — local library, instant, no network
  • Holiday *names* → NSE live API (current year) + FIXED_HOLIDAY_NAMES (fixed dates)
                      persisted in a lightweight JSON file so names survive across runs
  • Fallback name   → "NSE Holiday"

Coverage: 2007 onwards (XBOM calendar starts 2006-07-03; 2006 is partial so skipped).
"""

import json
import os
import re
import warnings
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

warnings.filterwarnings("ignore")

try:
    import exchange_calendars as xcals
    _HAS_EXCHANGE_CALENDARS = True
except ImportError:
    _HAS_EXCHANGE_CALENDARS = False
    print("Warning: exchange_calendars not installed. Install: pip install exchange_calendars")

try:
    import openpyxl  # noqa: F401
except ImportError:
    print("Warning: openpyxl not installed. Excel export will fail. Install: pip install openpyxl")


# ---------------------------------------------------------------------------
# Fixed-date holidays — same calendar date every year, no lookup needed
# ---------------------------------------------------------------------------

FIXED_HOLIDAY_NAMES: Dict[tuple, str] = {
    (1, 26):  "Republic Day",
    (5,  1):  "Maharashtra Day / May Day",
    (8, 15):  "Independence Day",
    (10, 2):  "Gandhi Jayanti",
    (12, 25): "Christmas",
}

# Keyword → clean display name for fuzzy-matching NSE API raw text
_NSE_NAME_HINTS: Dict[str, str] = {
    "republic":      "Republic Day",
    "independence":  "Independence Day",
    "gandhi":        "Gandhi Jayanti",
    "christmas":     "Christmas",
    "maharashtra":   "Maharashtra Day",
    "good friday":   "Good Friday",
    "diwali":        "Diwali",
    "laxmi":         "Diwali (Laxmi Pujan)",
    "balipratipada": "Diwali-Balipratipada",
    "holi":          "Holi",
    "eid":           "Eid",
    "bakri":         "Bakri Id (Eid ul-Adha)",
    "muharram":      "Muharram",
    "janmashtami":   "Janmashtami",
    "ganesh":        "Ganesh Chaturthi",
    "dussehra":      "Dussehra",
    "navami":        "Ram Navami",
    "mahavir":       "Mahavir Jayanti",
    "ambedkar":      "Ambedkar Jayanti",
    "buddha":        "Buddha Purnima",
    "guru nanak":    "Guru Nanak Jayanti",
    "shivratri":     "Maha Shivratri",
}

_XBOM_CALENDAR_ID = "XBOM"
_XBOM_START_YEAR  = 2007   # 2006 is partial (calendar starts mid-year)


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class IndianStockHolidayScraper:
    """
    Derive NSE/BSE trading holidays on-the-fly from exchange_calendars (XBOM).

    No pickle cache — exchange_calendars is a local library so computing any
    year's holidays takes milliseconds. Names are stored in a small JSON file
    (data/holiday_names.json) so NSE live-API names persist across runs without
    needing to re-hit the API every time.
    """

    def __init__(self):
        self.nse_base_url = "https://www.nseindia.com"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept":          "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer":         "https://www.nseindia.com/",
        })
        self.current_year = datetime.now().year

        # Resolve data directory (project_root/data/)
        script_dir   = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(script_dir))
        data_dir     = Path(project_root) / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        # JSON file that persists NSE-API-sourced holiday names across runs
        self._names_file: Path = data_dir / "holiday_names.json"
        self._names: Dict[str, str] = self._load_names()

    # -----------------------------------------------------------------------
    # Name store (JSON — lightweight, human-readable, no pickle)
    # -----------------------------------------------------------------------

    def _load_names(self) -> Dict[str, str]:
        """Load persisted holiday names from JSON. Returns {} on first run."""
        if self._names_file.exists():
            try:
                with open(self._names_file) as f:
                    data = json.load(f)
                print(f"Loaded {len(data)} holiday names from {self._names_file}")
                return data
            except Exception as e:
                print(f"Warning: could not read {self._names_file}: {e}")
        return {}

    def _save_names(self) -> None:
        """Persist holiday names to JSON."""
        try:
            with open(self._names_file, "w") as f:
                json.dump(self._names, f, indent=2, sort_keys=True)
            print(f"✓ Saved {len(self._names)} holiday names to {self._names_file}")
        except Exception as e:
            print(f"Warning: could not save names file: {e}")

    # -----------------------------------------------------------------------
    # NSE live API — name enrichment
    # -----------------------------------------------------------------------

    def refresh_names(self) -> None:
        """
        Fetch holiday names for the current year from the NSE live API and
        merge them into the persisted names JSON.

        Only needs to be called once per year (or when you want the latest
        NSE-official names). After that, names load instantly from the JSON.
        """
        print(f"\nFetching holiday names from NSE live API for {self.current_year}...")
        names = self._fetch_nse_names(self.current_year)
        if names:
            self._names.update(names)
            self._save_names()
            print(f"✓ {len(names)} new names merged into {self._names_file.name}")
        else:
            print("✗ NSE live API returned no names (expected for past years).")

    def _fetch_nse_names(self, year: int) -> Dict[str, str]:
        """Return {YYYY-MM-DD: name} from NSE API, filtered to year."""
        try:
            self.session.get(f"{self.nse_base_url}/", timeout=10)
            for endpoint in (
                f"{self.nse_base_url}/api/holiday-master?type=trading",
                f"{self.nse_base_url}/api/holiday-master",
            ):
                try:
                    resp = self.session.get(endpoint, timeout=15)
                    if resp.status_code == 200:
                        names = self._parse_nse_names(resp.json(), year)
                        if names:
                            return names
                except Exception:
                    continue
        except Exception as e:
            print(f"  NSE API error: {e}")
        return {}

    def _parse_nse_names(self, data: dict, target_year: int) -> Dict[str, str]:
        names: Dict[str, str] = {}
        try:
            holiday_list = None
            if isinstance(data, dict):
                for key in ("trading", "CM", "FO", "CD", "holidays"):
                    if key in data:
                        holiday_list = data[key]
                        break
            elif isinstance(data, list):
                holiday_list = data

            if not holiday_list:
                return names

            for item in holiday_list:
                if not isinstance(item, dict):
                    continue
                raw_date = (item.get("tradingDate") or item.get("date")
                            or item.get("holidayDate", ""))
                raw_name = (item.get("description") or item.get("occasion")
                            or item.get("holiday", ""))
                if not raw_date or not raw_name:
                    continue
                parsed = self._parse_date_string(str(raw_date))
                if parsed and parsed.year == target_year:
                    names[parsed.strftime("%Y-%m-%d")] = raw_name.strip()
        except Exception as e:
            print(f"  NSE name parse error: {e}")
        return names

    # -----------------------------------------------------------------------
    # Name resolution
    # -----------------------------------------------------------------------

    def _resolve_name(self, dt: datetime) -> str:
        """
        Best-available name for a holiday date, in priority order:
          1. Persisted NSE API name (from holiday_names.json)
          2. Fixed-date holiday table
          3. "NSE Holiday" fallback
        """
        date_str = dt.strftime("%Y-%m-%d")

        if date_str in self._names:
            return self._clean_nse_name(self._names[date_str])

        key = (dt.month, dt.day)
        if key in FIXED_HOLIDAY_NAMES:
            return FIXED_HOLIDAY_NAMES[key]

        return "NSE Holiday"

    def _clean_nse_name(self, raw: str) -> str:
        lower = raw.lower()
        for keyword, clean in _NSE_NAME_HINTS.items():
            if keyword in lower:
                return clean
        return raw.strip()

    # -----------------------------------------------------------------------
    # Core — derive holidays directly from exchange_calendars
    # -----------------------------------------------------------------------

    def get_holidays(self, start_date: str = None,
                     end_date: str = None) -> List[Dict]:
        """
        Return NSE/BSE holidays between start_date and end_date (inclusive).

        Dates are derived live from exchange_calendars (XBOM) — a local Python
        library, so this requires no network access and completes in milliseconds.

        Args:
            start_date: 'YYYY-MM-DD' (default: Jan 1 of current year)
            end_date:   'YYYY-MM-DD' (default: Dec 31 of current year)

        Returns:
            List of dicts with keys: date, holiday, day, source, year
        """
        if not _HAS_EXCHANGE_CALENDARS:
            print("✗ exchange_calendars not installed.")
            return []

        if not start_date:
            start_date = f"{self.current_year}-01-01"
        if not end_date:
            end_date = f"{self.current_year}-12-31"

        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt   = datetime.strptime(end_date,   "%Y-%m-%d")

        # Clamp to XBOM minimum supported year
        if start_dt.year < _XBOM_START_YEAR:
            print(f"⚠ Clamping start to {_XBOM_START_YEAR} (XBOM calendar minimum).")
            start_dt = datetime(_XBOM_START_YEAR, 1, 1)
            start_date = start_dt.strftime("%Y-%m-%d")

        print(f"\n{'='*60}")
        print(f"Deriving holidays {start_date} → {end_date} from exchange_calendars")
        print(f"{'='*60}")

        try:
            cal      = xcals.get_calendar(_XBOM_CALENDAR_ID)
            sessions = cal.sessions_in_range(start_date, end_date)
            sessions_set = set(sessions.date)

            all_weekdays = pd.bdate_range(start_date, end_date)
            holidays: List[Dict] = []

            for ts in all_weekdays:
                dt = ts.to_pydatetime()
                if dt.date() not in sessions_set:
                    holidays.append({
                        "date":    dt.strftime("%Y-%m-%d"),
                        "holiday": self._resolve_name(dt),
                        "day":     dt.strftime("%A"),
                        "source":  "exchange_calendars/XBOM",
                        "year":    dt.year,
                    })

            print(f"Found {len(holidays)} holidays")
            return holidays

        except Exception as e:
            print(f"✗ exchange_calendars error: {e}")
            return []

    def get_holidays_dataframe(self, start_date: str = None,
                               end_date: str = None) -> pd.DataFrame:
        """Return holidays as a pandas DataFrame."""
        holidays = self.get_holidays(start_date, end_date)
        if holidays:
            df = pd.DataFrame(holidays)
            df["date"] = pd.to_datetime(df["date"])
            return df
        return pd.DataFrame(columns=["date", "holiday", "day", "source", "year"])

    # -----------------------------------------------------------------------
    # Export
    # -----------------------------------------------------------------------

    def export_to_excel(self, filename: str, start_date: str = None,
                        end_date: str = None) -> None:
        df = self.get_holidays_dataframe(start_date, end_date)
        if df.empty:
            print("✗ No holidays found for the specified date range")
            return

        if not filename.endswith(".xlsx"):
            filename = filename.rsplit(".", 1)[0] + ".xlsx"

        with pd.ExcelWriter(filename, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Stock Market Holidays", index=False)
            ws = writer.sheets["Stock Market Holidays"]
            for idx, col in enumerate(df.columns):
                max_len = max(df[col].astype(str).apply(len).max(), len(col)) + 2
                ws.column_dimensions[chr(65 + idx)].width = max_len

        print(f"\n✓ Exported {len(df)} holidays to {filename}")
        print(f"  Date range: {df['date'].min().date()} → {df['date'].max().date()}")

    def export_to_csv(self, filename: str, start_date: str = None,
                      end_date: str = None) -> None:
        df = self.get_holidays_dataframe(start_date, end_date)
        if df.empty:
            print("✗ No holidays found for the specified date range")
            return
        df.to_csv(filename, index=False)
        print(f"\n✓ Exported {len(df)} holidays to {filename}")

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def _parse_date_string(self, date_str: str) -> Optional[datetime]:
        date_str = re.sub(r"[^\w\s\-/.]", "", date_str.strip())
        for fmt in (
            "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y",
            "%d %b %Y", "%d %B %Y", "%b %d, %Y", "%B %d, %Y",
            "%d-%b-%Y", "%d-%B-%Y", "%Y/%m/%d", "%d.%m.%Y",
        ):
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    scraper = IndianStockHolidayScraper()

    # Refresh NSE live names once per year (or when you want fresh names).
    # Comment this out after the first run — names are persisted in JSON.
    scraper.refresh_names()

    # Export to Excel
    script_dir   = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    output_dir   = Path(project_root) / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    current_year = datetime.now().year
    scraper.export_to_excel(
        str(output_dir / "Sensex_Holidays.xlsx"),
        start_date=f"{_XBOM_START_YEAR}-01-01",
        end_date=f"{current_year}-12-31",
    )


if __name__ == "__main__":
    main()