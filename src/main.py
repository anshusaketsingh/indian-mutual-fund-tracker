"""
Creator: @Saket
Email: anshusaketsingh@gmail.com
"""

import argparse
import sys
import subprocess
from pathlib import Path

# Ensure the root project directory is on the path so we can import 'src'
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Automatically use the virtual environment Python to prevent ModuleNotFoundErrors
venv_dir = project_root / ".venv"
venv_python = venv_dir / "bin" / "python"

if not venv_python.exists():
    print("Virtual environment not found. Creating '.venv' automatically...")
    subprocess.check_call([sys.executable, "-m", "venv", str(venv_dir)])
    print("Installing dependencies from requirements.txt...")
    subprocess.check_call([
        str(venv_python), "-m", "pip", "install", "-r", str(project_root / "requirements.txt")
    ])
    print("Setup complete! Launching pipeline...\n")

if sys.prefix != str(venv_dir):
    sys.exit(subprocess.call([str(venv_python)] + sys.argv))

from src.orchestrator import DataPipeline

def main():

    # Launch GUI if no arguments are provided
    if len(sys.argv) == 1:
        print("Launching Streamlit GUI...")
        app_path = project_root / "src" / "app.py"
        sys.exit(subprocess.call([str(venv_python), "-m", "streamlit", "run", str(app_path)]))
        
    parser = argparse.ArgumentParser(description="Indian Mutual Fund Data Extractor & Exporter")
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD). Defaults to Jan 1st of current year.",
        default=None
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date (YYYY-MM-DD). Defaults to today.",
        default=None
    )
    parser.add_argument(
        "--format",
        nargs="+",
        choices=["hyper", "csv", "parquet", "sqlite"],
        default=["hyper"],
        help="Output format(s) to generate. You can specify multiple."
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=30,
        help="Number of threads for parallel API extraction."
    )

    args = parser.parse_args()

    from datetime import datetime
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = project_root / "output" / f"Run_{run_id}"
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Saving outputs to {output_dir}")

    print("\n======================================================================")
    print("EXTRACTING STOCK MARKET HOLIDAYS")
    print("======================================================================")
    from src.core.holidays import IndianStockHolidayScraper
    
    scraper = IndianStockHolidayScraper()
    scraper.refresh_names()
    
    s_date = args.start_date if args.start_date else "2007-01-01"
    e_date = args.end_date if args.end_date else f"{datetime.now().year}-12-31"
    
    s_date_str = s_date.replace("-", "")
    e_date_str = e_date.replace("-", "")
    base_name = f"Sensex_Holidays_{s_date_str}_{e_date_str}"
    
    df = scraper.get_holidays_dataframe(start_date=s_date, end_date=e_date)
    
    if not df.empty:
        if "csv" in args.format:
            df.to_csv(str(output_dir / f"{base_name}.csv"), index=False)
            print(f"Exported holidays to {output_dir / f'{base_name}.csv'}")
        if "parquet" in args.format:
            df.to_parquet(str(output_dir / f"{base_name}.parquet"), index=False)
            print(f"Exported holidays to {output_dir / f'{base_name}.parquet'}")

    pipeline = DataPipeline(max_workers=args.workers, output_dir=str(output_dir))
    pipeline.run(
        start_date=args.start_date,
        end_date=args.end_date,
        output_formats=args.format,
        holidays_df=df
    )

if __name__ == "__main__":
    main()
