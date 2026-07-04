"""
Creator: @Saket
Email: anshusaketsingh@gmail.com
"""

import json
import threading
from pathlib import Path
from datetime import datetime
import ast

import pandas as pd
import numpy as np

class CacheManager:
    """Manages Parquet-based caching for NAV records and scheme metadata."""
    def __init__(self, data_dir: str = "data"):
        self.cache_dir = Path(data_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.nav_cache_parquet = self.cache_dir / "nav_data_cache.parquet"
        self.metadata_cache_parquet = self.cache_dir / "scheme_metadata_cache.parquet"
        self.cache_metadata_file = self.cache_dir / "cache_metadata.json"
        
        self.cache_lock = threading.Lock()
        
        # Load state
        self.nav_cache_df = self._load_nav_cache()
        self.metadata_cache = self._load_metadata_cache()
        self._reset_cache_stats()

    # ------------------------------------------------------------------
    # Statistics Tracking
    # ------------------------------------------------------------------
    def _reset_cache_stats(self):
        self.cache_stats = {
            "hits": 0, "misses": 0, "metadata_hits": 0, "metadata_misses": 0, "new_data": 0
        }

    def increment_cache_stat(self, key: str, increment: int = 1):
        self.cache_stats.setdefault(key, 0)
        self.cache_stats[key] += increment

    def get_cache_stat(self, key: str, default: int = 0) -> int:
        return self.cache_stats.get(key, default)

    # ------------------------------------------------------------------
    # NAV Cache
    # ------------------------------------------------------------------
    def _load_nav_cache(self) -> pd.DataFrame:
        self.last_cache_update = None
        if self.nav_cache_parquet.exists():
            df = pd.read_parquet(self.nav_cache_parquet, engine="fastparquet")
            if self.cache_metadata_file.exists():
                try:
                    with open(self.cache_metadata_file) as f:
                        meta = json.load(f)
                    self.last_cache_update = meta.get('last_updated')
                    self.oldest_synced_date = meta.get('oldest_synced_date', '9999-12-31')
                except Exception:
                    self.oldest_synced_date = '9999-12-31'
            return df
        self.oldest_synced_date = '9999-12-31'
        return pd.DataFrame(columns=["Scheme_Code", "Date", "NAV"])

    def save_nav_cache(self):
        try:
            with self.cache_lock:
                if self.nav_cache_df is None or self.nav_cache_df.empty:
                    return
                
                # De-duplicate
                before_len = len(self.nav_cache_df)
                self.nav_cache_df.drop_duplicates(subset=["Scheme_Code", "Date"], keep="last", inplace=True)
                
                self.nav_cache_df.to_parquet(self.nav_cache_parquet, engine="fastparquet", index=False)
                
                unique_schemes = self.nav_cache_df["Scheme_Code"].nunique()
                total_records = len(self.nav_cache_df)
                
                meta = {
                    "last_updated": datetime.now().isoformat(),
                    "schemes_cached": unique_schemes,
                    "total_records": total_records,
                    "cache_policy": "parquet_accumulative",
                    "oldest_synced_date": getattr(self, 'oldest_synced_date', '9999-12-31')
                }
                with open(self.cache_metadata_file, "w") as f:
                    json.dump(meta, f, indent=2)

        except Exception as e:
            print(f"Warning: Error saving NAV cache: {e}")

    # ------------------------------------------------------------------
    # Metadata Cache
    # ------------------------------------------------------------------
    def _load_metadata_cache(self) -> dict:
        if self.metadata_cache_parquet.exists():
            try:
                df = pd.read_parquet(self.metadata_cache_parquet, engine="fastparquet")
                df = df.set_index('Scheme_Code')
                df = df.replace({np.nan: None})
                res = df.to_dict(orient='index')
                for code, data in res.items():
                    for key in ["raw_details", "raw_quote", "processed_metadata"]:
                        if key in data and isinstance(data[key], str) and data[key] not in ("None", ""):
                            try:
                                data[key] = ast.literal_eval(data[key])
                            except Exception:
                                pass
                return res
            except Exception as e:
                print(f"Warning: Failed to load parquet metadata cache: {e}")
                return {}
        return {}

    def save_metadata_cache(self):
        try:
            with self.cache_lock:
                if not self.metadata_cache:
                    return
                df = pd.DataFrame.from_dict(self.metadata_cache, orient='index')
                df.index.name = 'Scheme_Code'
                df = df.reset_index()
                for col in df.columns:
                    df[col] = df[col].astype(str)
                df.to_parquet(self.metadata_cache_parquet, engine="fastparquet", index=False)
        except Exception as e:
            print(f"Warning: Error saving metadata cache: {e}")

    def get_cached_metadata(self, scheme_code: str) -> dict:
        if scheme_code in self.metadata_cache:
            self.increment_cache_stat("metadata_hits")
            return self.metadata_cache[scheme_code]
        self.increment_cache_stat("metadata_misses")
        return None

    def update_metadata_cache(self, scheme_code: str, scheme_details: dict, nav_quote: dict, processed_metadata: dict):
        with self.cache_lock:
            self.metadata_cache[str(scheme_code)] = {
                "raw_details": scheme_details,
                "raw_quote": nav_quote,
                "processed_metadata": processed_metadata
            }

    # ------------------------------------------------------------------
    # Freshness
    # ------------------------------------------------------------------
    def is_cache_fresh(self, hours: int = 12) -> bool:
        if not getattr(self, "last_cache_update", None):
            return False
        try:
            last_dt = datetime.fromisoformat(self.last_cache_update)
            if (datetime.now() - last_dt).total_seconds() < hours * 3600:
                return True
        except Exception:
            pass
        return False
