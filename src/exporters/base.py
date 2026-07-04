"""
Creator: @Saket
Email: anshusaketsingh@gmail.com
"""

from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path

class BaseExporter(ABC):
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def export(self, metadata_df: pd.DataFrame, nav_df: pd.DataFrame, filename: str) -> bool:
        """Export the DataFrames to the specified file format."""
        pass
