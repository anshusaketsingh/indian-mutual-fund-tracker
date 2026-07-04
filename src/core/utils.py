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
    
    # Check Index/ETF first as they can sometimes contain 'equity' or 'debt' in the name
    if "index" in cat_lower or "etf" in cat_lower or "exchange traded" in cat_lower:
        return "Index/ETF"
    if "liquid" in cat_lower or "overnight" in cat_lower:
        return "Liquid/Money Market"
    if "equity" in cat_lower:
        return "Equity"
    if "debt" in cat_lower:
        return "Debt"
    if "hybrid" in cat_lower:
        return "Hybrid"
    if "solution" in cat_lower:
        return "Solution Oriented"
    return "Others"

def extract_sub_category(scheme_category: str) -> str:
    if not scheme_category:
        return "Unknown"
    
    parts = scheme_category.split(" - ")
    if len(parts) > 1:
        return parts[1].strip()
    
    return scheme_category.strip()

def extract_plan_type(scheme_name: str) -> str:
    if not scheme_name:
        return "Unknown"
    
    name_lower = scheme_name.lower()
    if "idcw" in name_lower:
        return "IDCW"
    if "dividend" in name_lower:
        return "Dividend"
    if "bonus" in name_lower:
        return "Bonus"
    if "growth" in name_lower:
        return "Growth"
        
    return "Others"

def extract_investment_plan(scheme_name: str) -> str:
    if not scheme_name:
        return "Unknown"
    
    name_lower = scheme_name.lower()
    if "direct" in name_lower:
        return "Direct"
    if "regular" in name_lower:
        return "Regular"
        
    # Pure ETFs don't have Direct/Regular plans since they trade on exchanges
    if "etf" in name_lower or "exchange traded" in name_lower or "bees" in name_lower:
        if "fof" not in name_lower and "fund of" not in name_lower:
            return "ETF"
            
    return "Others"

import re

def extract_clean_fund_name(scheme_name: str) -> str:
    if not scheme_name:
        return "Unknown"
        
    # 1. Split by hyphen, taking the first part. This safely handles 90% of AMFI strings.
    clean = scheme_name.split("-")[0].strip()
    
    # 2. Iteratively strip trailing modifiers (for funds that omit the hyphen, like ICICI)
    modifiers = [
        "direct", "regular", "growth", "idcw", "dividend", 
        "bonus", "retail", "option", "plan", "payout", "reinvestment", 
        "daily", "weekly", "monthly", "quarterly", "half yearly", "yearly", "annual"
    ]
    pattern = r'(?i)\s+\b(?:' + '|'.join(modifiers) + r')\b'
    
    # Keep stripping from the end while there's a match at the end
    while True:
        new_clean = re.sub(pattern + r'$', '', clean).strip()
        if new_clean == clean:
            break
        clean = new_clean
        
    # Apply proper Title Casing while preserving acronyms (like HDFC, ETF, etc.)
    if clean:
        words = []
        for w in clean.split():
            # If word is fully uppercase, we preserve it (e.g. HDFC, SBI, ETF)
            # Exception: MUTUAL and FUND should be title cased "Mutual Fund"
            if w.upper() in ["MUTUAL", "FUND"]:
                words.append(w.title())
            elif w.isupper():
                words.append(w)
            else:
                words.append(w.title())
        clean = " ".join(words)
        
    return clean
