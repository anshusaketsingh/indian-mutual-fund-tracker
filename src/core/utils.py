"""
Creator: @Saket
Email: anshusaketsingh@gmail.com
"""

from datetime import datetime
import pandas as pd

def get_default_date_range():
    today = datetime.now()
    start_date = datetime(today.year, 1, 1)
    end_date = datetime.combine(today, datetime.min.time())
    return start_date, end_date

def parse_date_input(date_input) -> datetime:
    if isinstance(date_input, datetime):
        return date_input
    elif isinstance(date_input, str):
        for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                return datetime.strptime(date_input, fmt)
            except ValueError:
                continue
        try:
            return datetime.strptime(date_input, "%Y-%m-%d")
        except ValueError:
            return pd.to_datetime(date_input).to_pydatetime()
    raise ValueError(f"Unrecognized date format: {date_input}")

def validate_date_range(start_date, end_date):
    start_date = parse_date_input(start_date)
    end_date = parse_date_input(end_date)
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    
    today = datetime.now()
    if end_date.date() > today.date():
        end_date = datetime.combine(today, datetime.min.time())
    return start_date, end_date

def normalize_filter(filter_val):
    if not filter_val:
        return []
    if isinstance(filter_val, str):
        return [x.strip() for x in filter_val.split(",")]
    return list(filter_val)

def classify_category(scheme_category: str) -> str:
    if not scheme_category:
        return "Others"
    cat_lower = scheme_category.lower()
    if "equity" in cat_lower:
        return "Equity"
    if "debt" in cat_lower:
        return "Debt"
    if "hybrid" in cat_lower:
        return "Hybrid"
    if "index" in cat_lower or "etf" in cat_lower:
        return "Index/ETF"
    if "liquid" in cat_lower:
        return "Liquid"
    return "Others"
