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
