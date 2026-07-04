"""
Creator: @Saket
Email: anshusaketsingh@gmail.com
"""

import pandas as pd
from tableauhyperapi import (
    HyperProcess, Connection, Telemetry, CreateMode,
    TableDefinition, SqlType, Inserter
)
from .base import BaseExporter

class HyperExporter(BaseExporter):
    def export(self, metadata_df: pd.DataFrame, nav_df: pd.DataFrame, filename: str) -> bool:
        output_filepath = str(self.output_dir / filename)
        print(f"\nCreating Tableau Hyper file: {output_filepath}")

        try:
            with HyperProcess(
                telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
                parameters={"log_config": ""}
            ) as hyper:
                with Connection(
                    endpoint=hyper.endpoint,
                    database=output_filepath,
                    create_mode=CreateMode.CREATE_AND_REPLACE,
                ) as conn:

                    metadata_table = TableDefinition(
                        table_name="MutualFund_Metadata",
                        columns=[
                            TableDefinition.Column("Scheme_Code",            SqlType.text()),
                            TableDefinition.Column("Scheme_Name",            SqlType.text()),
                            TableDefinition.Column("Fund_House",             SqlType.text()),
                            TableDefinition.Column("Scheme_Type",            SqlType.text()),
                            TableDefinition.Column("Scheme_Category",        SqlType.text()),
                            TableDefinition.Column("Scheme_Start_Date_Info", SqlType.text()),
                            TableDefinition.Column("Current_NAV",            SqlType.text()),
                            TableDefinition.Column("Last_Updated",           SqlType.text()),
                            TableDefinition.Column("Main_Category",          SqlType.text()),
                        ]
                    )

                    nav_table = TableDefinition(
                        table_name="MutualFund_NAV",
                        columns=[
                            TableDefinition.Column("Scheme_Code", SqlType.text()),
                            TableDefinition.Column("Date",        SqlType.date()),
                            TableDefinition.Column("NAV",         SqlType.double()),
                            TableDefinition.Column("Year",        SqlType.int()),
                            TableDefinition.Column("Month",       SqlType.int()),
                            TableDefinition.Column("Day",         SqlType.int()),
                            TableDefinition.Column("Weekday",     SqlType.text()),
                        ]
                    )

                    conn.catalog.create_table(metadata_table)
                    conn.catalog.create_table(nav_table)
                    print("Tables created.")

                    if not metadata_df.empty:
                        print("Inserting metadata...")
                        text_cols = [
                            "Scheme_Code", "Scheme_Name", "Fund_House", "Scheme_Type",
                            "Scheme_Category", "Scheme_Start_Date_Info",
                            "Current_NAV", "Last_Updated", "Main_Category",
                        ]
                        rows = []
                        for t in metadata_df[text_cols].itertuples(index=False):
                            rows.append([
                                str(v) if (v is not None and not (isinstance(v, float) and pd.isna(v))) else None
                                for v in t
                            ])

                        batch_size = 5_000
                        with Inserter(conn, metadata_table) as ins:
                            for i in range(0, len(rows), batch_size):
                                for row in rows[i : i + batch_size]:
                                    ins.add_row(row)
                                print(f"  Metadata rows inserted: {min(i+batch_size, len(rows))}")
                            ins.execute()
                        print(f"Inserted {len(rows)} metadata records.")

                    if not nav_df.empty:
                        print("Inserting NAV data...")
                        nav_insert = nav_df.copy()
                        dt_series = pd.to_datetime(nav_insert["Date"])
                        nav_insert["DateParsed"]  = dt_series.dt.date
                        nav_insert["NAVParsed"]   = pd.to_numeric(nav_insert["NAV"], errors="coerce")
                        nav_insert["YearParsed"]  = dt_series.dt.year.astype("Int32")
                        nav_insert["MonthParsed"] = dt_series.dt.month.astype("Int32")
                        nav_insert["DayParsed"]   = dt_series.dt.day.astype("Int32")
                        nav_insert["WeekdayParsed"] = dt_series.dt.day_name()

                        batch_size = 50_000
                        with Inserter(conn, nav_table) as ins:
                            for i in range(0, len(nav_insert), batch_size):
                                chunk = nav_insert.iloc[i : i + batch_size]
                                for t in chunk.itertuples(index=False):
                                    try:
                                        nav_val = t.NAVParsed
                                        ins.add_row([
                                            str(t.Scheme_Code),
                                            t.DateParsed,
                                            nav_val if (nav_val == nav_val and 0 < nav_val < 100_000) else None,
                                            t.YearParsed  if pd.notna(t.YearParsed)  else None,
                                            t.MonthParsed if pd.notna(t.MonthParsed) else None,
                                            t.DayParsed   if pd.notna(t.DayParsed)   else None,
                                            str(t.WeekdayParsed) if pd.notna(t.WeekdayParsed) else None
                                        ])
                                    except Exception as e:
                                        print(f"Error row: {t}, Exception: {e}")
                                print(f"  NAV rows inserted: {min(i+batch_size, len(nav_insert))}")
                            ins.execute()
                        print(f"Inserted {len(nav_insert)} NAV records.")

            print(f"Hyper file created: {output_filepath}")
            return True
        except Exception as e:
            print(f"Error creating Hyper file: {e}")
            return False
