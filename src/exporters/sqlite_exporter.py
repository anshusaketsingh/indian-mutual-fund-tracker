"""
Creator: @Saket
Email: anshusaketsingh@gmail.com
"""

import sqlite3
import pandas as pd
from .base import BaseExporter

class SQLiteExporter(BaseExporter):
    def export(self, metadata_df: pd.DataFrame, nav_df: pd.DataFrame, filename: str) -> bool:
        output_filepath = str(self.output_dir / filename)
        print(f"\nCreating SQLite database: {output_filepath}")
        try:
            with sqlite3.connect(output_filepath) as conn:
                metadata_df.to_sql("MutualFund_Metadata", conn, if_exists="replace", index=False)
                nav_df.to_sql("MutualFund_NAV", conn, if_exists="replace", index=False)
                
                # Create indices for better query performance
                conn.execute("CREATE INDEX IF NOT EXISTS idx_nav_scheme ON MutualFund_NAV (Scheme_Code)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_nav_date ON MutualFund_NAV (Date)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_meta_scheme ON MutualFund_Metadata (Scheme_Code)")
                
            print(f"SQLite database created: {output_filepath}")
            return True
        except Exception as e:
            print(f"Error creating SQLite database: {e}")
            return False
