"""
Creator: @Saket
Email: anshusaketsingh@gmail.com
"""

import pandas as pd
from .base import BaseExporter

class CSVExporter(BaseExporter):
    def export(self, metadata_df: pd.DataFrame, nav_df: pd.DataFrame, filename: str, holidays_df: pd.DataFrame = None) -> bool:
        output_filepath = str(self.output_dir / filename)
        print(f"\nCreating CSV file: {output_filepath}")
        try:
            merged_df = pd.merge(
                nav_df,
                metadata_df[["Scheme_Code", "Scheme_Name", "Fund_House", "Main_Category"]],
                on="Scheme_Code",
                how="left"
            )
            merged_df.to_csv(output_filepath, index=False)
            print(f"CSV file created: {output_filepath}")
            return True
        except Exception as e:
            print(f"Error creating CSV file: {e}")
            return False
