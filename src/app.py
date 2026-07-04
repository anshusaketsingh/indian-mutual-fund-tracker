"""
Creator: @Saket
Email: anshusaketsingh@gmail.com
"""

import streamlit as st
import datetime
import os
import sys
from pathlib import Path

# Ensure src is accessible
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.orchestrator import DataPipeline
from src.core.holidays import IndianStockHolidayScraper

st.set_page_config(page_title="Indian Mutual Fund Tracker", layout="centered")

st.title("Indian Mutual Fund Tracker")
st.write("Extract NAV data and Stock Market Holidays.")

# Options
extract_nav = st.checkbox("Extract NAV Data", value=True)
extract_holidays = st.checkbox("Extract Stock Market Holidays", value=True)

# Dates
col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date", datetime.date(2007, 1, 1), min_value=datetime.date(2007, 1, 1), max_value=datetime.date.today(), format="DD/MM/YYYY")
with col2:
    end_date = st.date_input("End Date", datetime.date.today(), min_value=datetime.date(2007, 1, 1), max_value=datetime.date.today(), format="DD/MM/YYYY")

# Formats
export_formats = st.multiselect(
    "Export Formats (NAV)",
    ["hyper", "csv", "parquet", "sqlite"],
    default=["hyper"]
)

if st.button("Start Extraction"):
    if not extract_nav and not extract_holidays:
        st.warning("Please select at least one operation to perform.")
    else:
        st.write("### Progress")
        
        run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        run_output_dir = project_root / "output" / f"Mutual_Fund_Data_{run_id}"
        run_output_dir.mkdir(parents=True, exist_ok=True)
        
        hol_progress_text = st.empty()
        hol_progress = st.empty()
        
        meta_progress_text = st.empty()
        meta_progress = st.empty()
        
        nav_progress_text = st.empty()
        nav_progress = st.empty()

        if extract_holidays:
            hol_progress_text.text("Sensex Holidays: 0%")
            h_bar = hol_progress.progress(0.0)
            
            with st.spinner("Extracting Sensex Holidays..."):
                h_bar.progress(0.5)
                hol_progress_text.text("Sensex Holidays: Fetching...")
                
                scraper = IndianStockHolidayScraper()
                scraper.refresh_names()
                
                s_date = start_date.strftime("%Y-%m-%d")
                e_date = end_date.strftime("%Y-%m-%d")
                
                s_date_str = start_date.strftime("%Y%m%d")
                e_date_str = end_date.strftime("%Y%m%d")
                base_name = f"Sensex_Holidays_{s_date_str}_{e_date_str}"
                
                df = scraper.get_holidays_dataframe(start_date=s_date, end_date=e_date)
                if not df.empty:
                    if "csv" in export_formats:
                        df.to_csv(str(run_output_dir / f"{base_name}.csv"), index=False)
                    if "parquet" in export_formats:
                        df.to_parquet(str(run_output_dir / f"{base_name}.parquet"), index=False)
                    if "sqlite" in export_formats:
                        import sqlite3
                        conn = sqlite3.connect(str(run_output_dir / f"{base_name}.sqlite"))
                        df.to_sql("holidays", conn, if_exists="replace", index=False)
                        conn.close()
                    if "hyper" in export_formats:
                        import pantab
                        pantab.frames_to_hyper({"holidays": df}, str(run_output_dir / f"{base_name}.hyper"))
                
                h_bar.progress(1.0)
                hol_progress_text.text("Sensex Holidays: Complete!")

        if extract_nav:
            meta_progress_text.text("Metadata: 0%")
            m_bar = meta_progress.progress(0)
            
            nav_progress_text.text("NAV: 0%")
            n_bar = nav_progress.progress(0)
            
            import time
            start_time = time.time()
            
            def combined_cb(current, total, desc):
                if total > 0:
                    pct = current / total
                    if "Metadata" in desc:
                        m_bar.progress(pct)
                        meta_progress_text.text(f"Metadata: {current}/{total} ({(pct*100):.1f}%)")
                    elif "NAV" in desc:
                        n_bar.progress(pct)
                        elapsed = time.time() - start_time
                        if current > 0:
                            eta_seconds = (elapsed / current) * (total - current)
                            eta_mins = int(eta_seconds // 60)
                            eta_secs = int(eta_seconds % 60)
                            nav_progress_text.text(f"NAV: {current}/{total} ({(pct*100):.1f}%) | ETA: {eta_mins}m {eta_secs}s")
                        else:
                            nav_progress_text.text(f"NAV: {current}/{total} ({(pct*100):.1f}%) | ETA: Calculating...")

            with st.spinner("Extracting NAV Data..."):
                pipeline = DataPipeline(max_workers=30, output_dir=str(run_output_dir))
                pipeline.run(
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d"),
                    output_formats=export_formats,
                    progress_callback=combined_cb
                )
            m_bar.progress(1.0)
            meta_progress_text.text("Metadata: Complete!")
            n_bar.progress(1.0)
            nav_progress_text.text("NAV: Complete!")
                
        st.success(f"Extraction Finished Successfully! Files saved to output/{run_output_dir.name}")
