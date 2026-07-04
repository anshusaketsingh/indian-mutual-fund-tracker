"""
Creator: @Saket
Email: anshusaketsingh@gmail.com
"""

import time
from datetime import datetime

from .core.api import MftoolWrapper
from .core.cache import CacheManager
from .core.fetchers import MetadataFetcher, NavFetcher
from .exporters import CSVExporter, ParquetExporter, SQLiteExporter, HyperExporter

class DataPipeline:
    def __init__(self, max_workers: int = 10, api_delay: float = 0.05, data_dir: str = "data", output_dir: str = "output"):
        self.api = MftoolWrapper(api_delay=api_delay)
        self.cache = CacheManager(data_dir=data_dir)
        self.metadata_fetcher = MetadataFetcher(self.api, self.cache, max_workers)
        self.nav_fetcher = NavFetcher(self.api, self.cache, max_workers)
        
        self.output_dir = output_dir

        self.exporters = {
            "csv": CSVExporter(output_dir),
            "parquet": ParquetExporter(output_dir),
            "sqlite": SQLiteExporter(output_dir),
            "hyper": HyperExporter(output_dir)
        }

    def run(self, start_date=None, end_date=None, output_formats=["hyper"], progress_callback=None, holidays_df=None):
        print("======================================================================")
        print("MUTUAL FUND DATA EXTRACTION PIPELINE")
        print("======================================================================")
        
        start_time = time.time()
        
        metadata_df = self.metadata_fetcher.get_metadata(progress_callback=progress_callback)
        if metadata_df.empty:
            print("No metadata found.")
            return

        scheme_codes = metadata_df["Scheme_Code"].tolist()
        
        print(f"\nTotal funds identified: {len(metadata_df)}")
        
        nav_df = self.nav_fetcher.get_daily_nav(scheme_codes, start_date, end_date, progress_callback=progress_callback)
        
        # Enrich metadata with exact 1-Year Ago NAV & Date
        if not nav_df.empty:
            metadata_df = self._enrich_metadata(metadata_df, nav_df)
            
        print("\n======================================================================")
        print("EXPORTING DATA")
        print("======================================================================")
        if nav_df.empty:
            print("Warning: No NAV data found for the specified period.")
            return

        safe_start = start_date.replace("-", "") if isinstance(start_date, str) else "AllTime"
        safe_end   = end_date.replace("-", "") if isinstance(end_date, str) else "Now"
        if isinstance(start_date, datetime): safe_start = start_date.strftime("%Y%m%d")
        if isinstance(end_date, datetime):   safe_end = end_date.strftime("%Y%m%d")
        base_name = f"Indian_Mutual_Funds_NAV_{safe_start}_{safe_end}"

        for fmt in set(f.lower().strip() for f in output_formats):
            if fmt in self.exporters:
                self.exporters[fmt].export(metadata_df, nav_df, f"{base_name}.{fmt}", holidays_df)
            else:
                print(f"Warning: Unknown format '{fmt}' requested.")

        elapsed = time.time() - start_time
        print("\n======================================================================")
        print(f"PIPELINE COMPLETE in {elapsed:.1f}s")
        print("======================================================================")

    def _enrich_metadata(self, metadata_df, nav_df):
        import pandas as pd
        
        # Ensure proper datatypes
        nav_asof = nav_df[["Scheme_Code", "Date", "NAV"]].copy()
        nav_asof["Date_dt"] = pd.to_datetime(nav_asof["Date"], format="%Y-%m-%d")
        
        # Sort values properly for merge_asof
        nav_asof = nav_asof.sort_values("Date_dt")
        
        # Get the latest NAV for each fund
        latest_navs = nav_asof.groupby("Scheme_Code").tail(1).copy()
        latest_navs = latest_navs.rename(columns={"NAV": "Current_NAV_Value", "Date_dt": "Current_NAV_Date_dt"})
        latest_navs["Current_NAV_Date"] = latest_navs["Current_NAV_Date_dt"].dt.strftime("%Y-%m-%d")
        
        intervals = [
            ("1_Month", pd.DateOffset(months=1)),
            ("3_Month", pd.DateOffset(months=3)),
            ("6_Month", pd.DateOffset(months=6)),
            ("1_Year", pd.DateOffset(years=1)),
            ("3_Year", pd.DateOffset(years=3)),
            ("5_Year", pd.DateOffset(years=5)),
            ("10_Year", pd.DateOffset(years=10)),
            ("20_Year", pd.DateOffset(years=20)),
        ]
        
        # Drop the placeholder Current_NAV if it exists to avoid _x and _y suffixing
        if "Current_NAV" in metadata_df.columns:
            metadata_df = metadata_df.drop(columns=["Current_NAV"])
            
        # Merge latest NAV data
        metadata_df = metadata_df.merge(
            latest_navs[["Scheme_Code", "Current_NAV_Value", "Current_NAV_Date"]], 
            on="Scheme_Code", 
            how="left"
        )
        metadata_df = metadata_df.rename(columns={"Current_NAV_Value": "Current_NAV"})
        
        # Iterate and merge historical data for each time interval
        for prefix, offset in intervals:
            temp_latest = latest_navs[["Scheme_Code", "Current_NAV_Date_dt"]].copy()
            temp_latest["Target_Date"] = temp_latest["Current_NAV_Date_dt"] - offset
            temp_latest = temp_latest.sort_values("Target_Date")
            
            merged = pd.merge_asof(
                temp_latest,
                nav_asof,
                left_on="Target_Date",
                right_on="Date_dt",
                by="Scheme_Code",
                direction="backward"
            )
            
            merged[f"{prefix}_Ago_Date"] = merged["Date_dt"].dt.strftime("%Y-%m-%d")
            merged[f"{prefix}_Ago_NAV"] = merged["NAV"]
            
            metadata_df = metadata_df.merge(
                merged[["Scheme_Code", f"{prefix}_Ago_Date", f"{prefix}_Ago_NAV"]], 
                on="Scheme_Code", 
                how="left"
            )
            
        return metadata_df
