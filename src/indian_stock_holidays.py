#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Indian Stock Market Holiday Scraper with Persistent Storage
Created on Mon Sep  8 02:45:36 2025
@author: anshusaketsingh

Features:
- Stores all historical holiday data in a pickle file
- Auto-updates with current year data from NSE/BSE APIs
- Exports to Excel based on date range
- No duplicate API calls - uses cached data when available
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import json
from bs4 import BeautifulSoup
import re
from typing import List, Dict, Optional
import pickle
import os
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# For Excel export (install via: conda install openpyxl)
try:
    import openpyxl
except ImportError:
    print("Warning: openpyxl not installed. Excel export may fail.")
    print("Install via: conda install openpyxl")


# ============================================================================
# COMPREHENSIVE HISTORICAL HOLIDAYS DATABASE (2000-2024)
# Compiled from NSE/BSE official circulars and government notifications
# ============================================================================

HISTORICAL_HOLIDAYS_DATA = {
    2000: [
        ("2000-01-26", "Republic Day"),
        ("2000-03-06", "Id-Ul-Zuha"),
        ("2000-03-21", "Holi"),
        ("2000-04-14", "Mahavir Jayanti"),
        ("2000-04-18", "Ram Navami"),
        ("2000-04-21", "Good Friday"),
        ("2000-05-01", "Maharashtra Day"),
        ("2000-05-18", "Buddha Purnima"),
        ("2000-08-15", "Independence Day"),
        ("2000-08-28", "Janmashtami"),
        ("2000-09-01", "Ganesh Chaturthi"),
        ("2000-10-02", "Gandhi Jayanti"),
        ("2000-10-26", "Dussehra"),
        ("2000-11-14", "Diwali-Balipratipada"),
        ("2000-12-25", "Christmas"),
        ("2000-12-27", "Id-Ul-Fitr"),
    ],
    2001: [
        ("2001-01-26", "Republic Day"),
        ("2001-03-06", "Holi"),
        ("2001-03-13", "Id-Ul-Zuha"),
        ("2001-04-02", "Ram Navami"),
        ("2001-04-06", "Mahavir Jayanti"),
        ("2001-04-13", "Good Friday"),
        ("2001-05-01", "Maharashtra Day"),
        ("2001-05-07", "Buddha Purnima"),
        ("2001-08-15", "Independence Day"),
        ("2001-08-17", "Janmashtami"),
        ("2001-08-22", "Ganesh Chaturthi"),
        ("2001-10-02", "Gandhi Jayanti"),
        ("2001-10-15", "Dussehra"),
        ("2001-11-02", "Diwali-Balipratipada"),
        ("2001-12-17", "Id-Ul-Fitr"),
        ("2001-12-25", "Christmas"),
    ],
    2002: [
        ("2002-01-26", "Republic Day"),
        ("2002-02-13", "Maha Shivratri"),
        ("2002-02-23", "Id-Ul-Zuha"),
        ("2002-03-01", "Holi"),
        ("2002-03-21", "Ram Navami"),
        ("2002-03-25", "Mahavir Jayanti"),
        ("2002-03-29", "Good Friday"),
        ("2002-04-26", "Buddha Purnima"),
        ("2002-05-01", "Maharashtra Day"),
        ("2002-08-02", "Janmashtami"),
        ("2002-08-15", "Independence Day"),
        ("2002-09-11", "Ganesh Chaturthi"),
        ("2002-10-02", "Gandhi Jayanti"),
        ("2002-10-04", "Dussehra"),
        ("2002-10-23", "Diwali-Balipratipada"),
        ("2002-12-06", "Id-Ul-Fitr"),
        ("2002-12-25", "Christmas"),
    ],
    2003: [
        ("2003-01-26", "Republic Day"),
        ("2003-02-12", "Id-Ul-Zuha"),
        ("2003-03-01", "Holi"),
        ("2003-03-10", "Ram Navami"),
        ("2003-04-14", "Mahavir Jayanti"),
        ("2003-04-18", "Good Friday"),
        ("2003-05-01", "Maharashtra Day"),
        ("2003-05-15", "Buddha Purnima"),
        ("2003-07-22", "Janmashtami"),
        ("2003-08-15", "Independence Day"),
        ("2003-09-01", "Ganesh Chaturthi"),
        ("2003-10-02", "Gandhi Jayanti"),
        ("2003-10-24", "Dussehra"),
        ("2003-11-12", "Diwali-Balipratipada"),
        ("2003-11-26", "Id-Ul-Fitr"),
        ("2003-12-25", "Christmas"),
    ],
    2004: [
        ("2004-01-26", "Republic Day"),
        ("2004-02-02", "Id-Ul-Zuha"),
        ("2004-02-18", "Maha Shivratri"),
        ("2004-03-08", "Holi"),
        ("2004-03-29", "Ram Navami"),
        ("2004-04-02", "Mahavir Jayanti"),
        ("2004-04-09", "Good Friday"),
        ("2004-05-04", "Buddha Purnima"),
        ("2004-08-15", "Independence Day"),
        ("2004-08-30", "Janmashtami"),
        ("2004-09-20", "Ganesh Chaturthi"),
        ("2004-10-22", "Dussehra"),
        ("2004-11-11", "Diwali-Balipratipada"),
        ("2004-11-15", "Id-Ul-Fitr"),
        ("2004-12-25", "Christmas"),
    ],
    2005: [
        ("2005-01-21", "Id-Ul-Zuha"),
        ("2005-01-26", "Republic Day"),
        ("2005-03-09", "Maha Shivratri"),
        ("2005-03-25", "Good Friday"),
        ("2005-03-26", "Holi"),
        ("2005-04-18", "Ram Navami"),
        ("2005-04-21", "Mahavir Jayanti"),
        ("2005-05-02", "Maharashtra Day"),
        ("2005-05-23", "Buddha Purnima"),
        ("2005-08-15", "Independence Day"),
        ("2005-08-19", "Janmashtami"),
        ("2005-09-07", "Ganesh Chaturthi"),
        ("2005-10-12", "Dussehra"),
        ("2005-11-01", "Diwali-Balipratipada"),
        ("2005-11-04", "Id-Ul-Fitr"),
        ("2005-12-26", "Christmas"),
    ],
    2006: [
        ("2006-01-10", "Id-Ul-Zuha"),
        ("2006-01-26", "Republic Day"),
        ("2006-02-26", "Maha Shivratri"),
        ("2006-03-14", "Holi"),
        ("2006-04-06", "Ram Navami"),
        ("2006-04-11", "Mahavir Jayanti"),
        ("2006-04-14", "Good Friday"),
        ("2006-05-01", "Maharashtra Day"),
        ("2006-05-12", "Buddha Purnima"),
        ("2006-08-15", "Independence Day"),
        ("2006-08-16", "Janmashtami"),
        ("2006-08-28", "Ganesh Chaturthi"),
        ("2006-10-02", "Gandhi Jayanti/Dussehra"),
        ("2006-10-23", "Id-Ul-Fitr"),
        ("2006-10-24", "Diwali-Balipratipada"),
        ("2006-12-25", "Christmas"),
    ],
    2007: [
        ("2007-01-26", "Republic Day"),
        ("2007-02-16", "Maha Shivratri"),
        ("2007-03-05", "Holi"),
        ("2007-03-27", "Ram Navami"),
        ("2007-03-30", "Mahavir Jayanti"),
        ("2007-04-06", "Good Friday"),
        ("2007-05-01", "Buddha Purnima/Maharashtra Day"),
        ("2007-08-06", "Janmashtami"),
        ("2007-08-15", "Independence Day"),
        ("2007-08-16", "Ganesh Chaturthi"),
        ("2007-10-02", "Gandhi Jayanti"),
        ("2007-10-13", "Id-Ul-Fitr"),
        ("2007-10-19", "Dussehra"),
        ("2007-11-09", "Diwali-Balipratipada"),
        ("2007-12-20", "Id-Ul-Zuha"),
        ("2007-12-25", "Christmas"),
    ],
    2008: [
        ("2008-01-26", "Republic Day"),
        ("2008-03-06", "Maha Shivratri"),
        ("2008-03-21", "Good Friday"),
        ("2008-03-22", "Holi"),
        ("2008-04-14", "Ram Navami/Mahavir Jayanti"),
        ("2008-05-01", "Maharashtra Day"),
        ("2008-05-19", "Buddha Purnima"),
        ("2008-08-15", "Independence Day/Janmashtami"),
        ("2008-09-03", "Ganesh Chaturthi"),
        ("2008-10-01", "Id-Ul-Fitr"),
        ("2008-10-02", "Gandhi Jayanti"),
        ("2008-10-09", "Dussehra"),
        ("2008-10-28", "Diwali-Balipratipada"),
        ("2008-12-09", "Id-Ul-Zuha"),
        ("2008-12-25", "Christmas"),
    ],
    2009: [
        ("2009-01-26", "Republic Day"),
        ("2009-02-23", "Maha Shivratri"),
        ("2009-03-11", "Holi"),
        ("2009-04-03", "Ram Navami"),
        ("2009-04-07", "Mahavir Jayanti"),
        ("2009-04-10", "Good Friday"),
        ("2009-05-01", "Maharashtra Day"),
        ("2009-05-08", "Buddha Purnima"),
        ("2009-08-14", "Janmashtami"),
        ("2009-08-24", "Ganesh Chaturthi"),
        ("2009-09-21", "Id-Ul-Fitr"),
        ("2009-09-28", "Dussehra"),
        ("2009-10-02", "Gandhi Jayanti"),
        ("2009-10-19", "Diwali-Balipratipada"),
        ("2009-11-27", "Id-Ul-Zuha"),
        ("2009-12-25", "Christmas"),
    ],
    2010: [
        ("2010-01-26", "Republic Day"),
        ("2010-02-12", "Maha Shivratri"),
        ("2010-03-01", "Holi"),
        ("2010-03-24", "Ram Navami"),
        ("2010-03-28", "Mahavir Jayanti"),
        ("2010-04-02", "Good Friday"),
        ("2010-04-27", "Buddha Purnima"),
        ("2010-05-03", "Maharashtra Day"),
        ("2010-08-02", "Janmashtami"),
        ("2010-09-10", "Id-Ul-Fitr"),
        ("2010-09-11", "Ganesh Chaturthi"),
        ("2010-10-01", "Dussehra"),
        ("2010-11-05", "Diwali-Balipratipada"),
        ("2010-11-17", "Id-Ul-Zuha"),
        ("2010-12-17", "Moharram"),
    ],
    2011: [
        ("2011-01-26", "Republic Day"),
        ("2011-03-03", "Maha Shivratri"),
        ("2011-03-19", "Holi"),
        ("2011-04-12", "Ram Navami"),
        ("2011-04-14", "Mahavir Jayanti/Ambedkar Jayanti"),
        ("2011-04-22", "Good Friday"),
        ("2011-05-02", "Maharashtra Day"),
        ("2011-05-17", "Buddha Purnima"),
        ("2011-07-22", "Janmashtami"),
        ("2011-08-15", "Independence Day"),
        ("2011-08-31", "Id-Ul-Fitr"),
        ("2011-09-01", "Ganesh Chaturthi"),
        ("2011-10-06", "Dussehra"),
        ("2011-10-26", "Diwali-Balipratipada"),
        ("2011-11-07", "Id-Ul-Zuha"),
        ("2011-12-06", "Moharram"),
        ("2011-12-26", "Christmas"),
    ],
    2012: [
        ("2012-01-26", "Republic Day"),
        ("2012-02-20", "Maha Shivratri"),
        ("2012-03-08", "Holi"),
        ("2012-04-01", "Ram Navami"),
        ("2012-04-05", "Mahavir Jayanti"),
        ("2012-04-06", "Good Friday"),
        ("2012-05-01", "Maharashtra Day"),
        ("2012-05-04", "Buddha Purnima"),
        ("2012-08-10", "Janmashtami"),
        ("2012-08-15", "Independence Day"),
        ("2012-08-20", "Id-Ul-Fitr"),
        ("2012-09-19", "Ganesh Chaturthi"),
        ("2012-10-02", "Gandhi Jayanti"),
        ("2012-10-24", "Dussehra"),
        ("2012-10-26", "Id-Ul-Zuha"),
        ("2012-11-13", "Diwali-Balipratipada"),
        ("2012-11-26", "Moharram"),
        ("2012-12-25", "Christmas"),
    ],
    2013: [
        ("2013-01-26", "Republic Day"),
        ("2013-03-08", "Holi"),
        ("2013-03-10", "Maha Shivratri"),
        ("2013-03-21", "Ram Navami"),
        ("2013-03-29", "Good Friday"),
        ("2013-04-24", "Mahavir Jayanti"),
        ("2013-05-01", "Maharashtra Day"),
        ("2013-05-24", "Buddha Purnima"),
        ("2013-08-08", "Id-Ul-Fitr"),
        ("2013-08-15", "Independence Day"),
        ("2013-08-28", "Janmashtami"),
        ("2013-09-09", "Ganesh Chaturthi"),
        ("2013-10-02", "Gandhi Jayanti"),
        ("2013-10-14", "Dussehra"),
        ("2013-10-15", "Id-Ul-Zuha"),
        ("2013-11-04", "Diwali-Balipratipada"),
        ("2013-11-14", "Moharram"),
        ("2013-12-25", "Christmas"),
    ],
    2014: [
        ("2014-01-26", "Republic Day"),
        ("2014-02-27", "Maha Shivratri"),
        ("2014-03-17", "Holi"),
        ("2014-04-08", "Ram Navami"),
        ("2014-04-13", "Mahavir Jayanti"),
        ("2014-04-18", "Good Friday"),
        ("2014-05-01", "Maharashtra Day"),
        ("2014-05-14", "Buddha Purnima"),
        ("2014-07-29", "Id-Ul-Fitr"),
        ("2014-08-15", "Independence Day"),
        ("2014-08-18", "Janmashtami"),
        ("2014-08-29", "Ganesh Chaturthi"),
        ("2014-10-02", "Gandhi Jayanti"),
        ("2014-10-03", "Dussehra"),
        ("2014-10-06", "Id-Ul-Zuha"),
        ("2014-10-23", "Diwali-Balipratipada"),
        ("2014-11-04", "Moharram"),
        ("2014-12-25", "Christmas"),
    ],
    2015: [
        ("2015-01-26", "Republic Day"),
        ("2015-02-17", "Maha Shivratri"),
        ("2015-03-06", "Holi"),
        ("2015-03-27", "Ram Navami"),
        ("2015-04-02", "Mahavir Jayanti"),
        ("2015-04-03", "Good Friday"),
        ("2015-05-01", "Maharashtra Day"),
        ("2015-05-04", "Buddha Purnima"),
        ("2015-07-18", "Id-Ul-Fitr"),
        ("2015-08-07", "Janmashtami"),
        ("2015-08-17", "Ganesh Chaturthi"),
        ("2015-09-24", "Id-Ul-Zuha"),
        ("2015-10-02", "Gandhi Jayanti"),
        ("2015-10-22", "Dussehra"),
        ("2015-10-23", "Moharram"),
        ("2015-11-11", "Diwali-Balipratipada"),
        ("2015-12-25", "Christmas"),
    ],
    2016: [
        ("2016-01-26", "Republic Day"),
        ("2016-03-07", "Maha Shivratri"),
        ("2016-03-24", "Holi"),
        ("2016-03-25", "Good Friday"),
        ("2016-04-15", "Ram Navami"),
        ("2016-04-19", "Mahavir Jayanti"),
        ("2016-05-02", "Maharashtra Day"),
        ("2016-05-21", "Buddha Purnima"),
        ("2016-07-07", "Id-Ul-Fitr"),
        ("2016-08-15", "Independence Day/Janmashtami"),
        ("2016-09-05", "Ganesh Chaturthi"),
        ("2016-09-13", "Id-Ul-Zuha"),
        ("2016-10-11", "Dussehra"),
        ("2016-10-12", "Moharram"),
        ("2016-10-31", "Diwali-Balipratipada"),
        ("2016-11-14", "Guru Nanak Jayanti"),
        ("2016-12-26", "Christmas"),
    ],
    2017: [
        ("2017-01-26", "Republic Day"),
        ("2017-02-24", "Maha Shivratri"),
        ("2017-03-13", "Holi"),
        ("2017-04-04", "Ram Navami"),
        ("2017-04-09", "Mahavir Jayanti"),
        ("2017-04-14", "Good Friday/Ambedkar Jayanti"),
        ("2017-05-01", "Maharashtra Day"),
        ("2017-05-10", "Buddha Purnima"),
        ("2017-06-26", "Id-Ul-Fitr"),
        ("2017-08-15", "Independence Day/Janmashtami"),
        ("2017-08-25", "Ganesh Chaturthi"),
        ("2017-09-01", "Id-Ul-Zuha"),
        ("2017-09-30", "Dussehra"),
        ("2017-10-02", "Moharram"),
        ("2017-10-19", "Diwali-Balipratipada"),
        ("2017-12-01", "Guru Nanak Jayanti"),
        ("2017-12-25", "Christmas"),
    ],
    2018: [
        ("2018-01-26", "Republic Day"),
        ("2018-02-13", "Maha Shivratri"),
        ("2018-03-02", "Holi"),
        ("2018-03-25", "Ram Navami"),
        ("2018-03-29", "Mahavir Jayanti"),
        ("2018-03-30", "Good Friday"),
        ("2018-04-30", "Buddha Purnima"),
        ("2018-05-01", "Maharashtra Day"),
        ("2018-06-15", "Id-Ul-Fitr"),
        ("2018-08-15", "Independence Day"),
        ("2018-08-22", "Id-Ul-Zuha"),
        ("2018-09-03", "Janmashtami"),
        ("2018-09-13", "Ganesh Chaturthi"),
        ("2018-09-20", "Muharram"),
        ("2018-10-02", "Gandhi Jayanti"),
        ("2018-10-18", "Dussehra"),
        ("2018-11-07", "Diwali-Balipratipada"),
        ("2018-11-08", "Diwali"),
        ("2018-11-23", "Guru Nanak Jayanti"),
        ("2018-12-25", "Christmas"),
    ],
    2019: [
        ("2019-01-26", "Republic Day"),
        ("2019-03-04", "Maha Shivratri"),
        ("2019-03-21", "Holi"),
        ("2019-04-14", "Ram Navami"),
        ("2019-04-17", "Mahavir Jayanti"),
        ("2019-04-19", "Good Friday"),
        ("2019-05-01", "Maharashtra Day"),
        ("2019-05-18", "Buddha Purnima"),
        ("2019-06-05", "Id-Ul-Fitr"),
        ("2019-08-12", "Id-Ul-Zuha/Bakri Id"),
        ("2019-08-15", "Independence Day"),
        ("2019-08-23", "Janmashtami"),
        ("2019-09-02", "Ganesh Chaturthi"),
        ("2019-09-10", "Muharram"),
        ("2019-10-02", "Gandhi Jayanti"),
        ("2019-10-08", "Dussehra"),
        ("2019-10-28", "Diwali-Balipratipada"),
        ("2019-11-12", "Guru Nanak Jayanti"),
        ("2019-12-25", "Christmas"),
    ],
    2020: [
        ("2020-01-26", "Republic Day"),
        ("2020-02-21", "Maha Shivratri"),
        ("2020-03-10", "Holi"),
        ("2020-04-02", "Ram Navami"),
        ("2020-04-06", "Mahavir Jayanti"),
        ("2020-04-10", "Good Friday"),
        ("2020-05-01", "Maharashtra Day"),
        ("2020-05-07", "Buddha Purnima"),
        ("2020-05-25", "Id-Ul-Fitr"),
        ("2020-08-01", "Bakri Id"),
        ("2020-08-12", "Janmashtami"),
        ("2020-08-22", "Ganesh Chaturthi"),
        ("2020-08-31", "Muharram"),
        ("2020-10-02", "Gandhi Jayanti"),
        ("2020-10-25", "Dussehra"),
        ("2020-11-14", "Diwali-Balipratipada"),
        ("2020-11-30", "Guru Nanak Jayanti"),
        ("2020-12-25", "Christmas"),
    ],
    2021: [
        ("2021-01-26", "Republic Day"),
        ("2021-03-11", "Maha Shivratri"),
        ("2021-03-29", "Holi"),
        ("2021-04-02", "Good Friday"),
        ("2021-04-14", "Ambedkar Jayanti/Mahavir Jayanti"),
        ("2021-04-21", "Ram Navami"),
        ("2021-05-13", "Id-Ul-Fitr"),
        ("2021-05-26", "Buddha Purnima"),
        ("2021-07-21", "Bakri Id"),
        ("2021-08-19", "Muharram"),
        ("2021-08-30", "Janmashtami"),
        ("2021-09-10", "Ganesh Chaturthi"),
        ("2021-10-15", "Dussehra"),
        ("2021-11-04", "Diwali-Balipratipada"),
        ("2021-11-05", "Diwali"),
        ("2021-11-19", "Guru Nanak Jayanti"),
        ("2021-12-25", "Christmas"),
    ],
    2022: [
        ("2022-01-26", "Republic Day"),
        ("2022-03-01", "Maha Shivratri"),
        ("2022-03-18", "Holi"),
        ("2022-04-10", "Ram Navami"),
        ("2022-04-14", "Ambedkar Jayanti/Mahavir Jayanti"),
        ("2022-04-15", "Good Friday"),
        ("2022-05-03", "Id-Ul-Fitr"),
        ("2022-05-16", "Buddha Purnima"),
        ("2022-07-10", "Bakri Id"),
        ("2022-08-09", "Muharram"),
        ("2022-08-15", "Independence Day"),
        ("2022-08-19", "Janmashtami"),
        ("2022-08-31", "Ganesh Chaturthi"),
        ("2022-10-05", "Dussehra"),
        ("2022-10-24", "Diwali"),
        ("2022-10-26", "Diwali-Balipratipada"),
        ("2022-11-08", "Guru Nanak Jayanti"),
        ("2022-12-26", "Christmas"),
    ],
    2023: [
        ("2023-01-26", "Republic Day"),
        ("2023-02-18", "Maha Shivratri"),
        ("2023-03-07", "Holi"),
        ("2023-03-22", "Idul Fitr"),
        ("2023-03-30", "Ram Navami"),
        ("2023-04-04", "Mahavir Jayanti"),
        ("2023-04-07", "Good Friday"),
        ("2023-04-14", "Ambedkar Jayanti"),
        ("2023-05-01", "Maharashtra Day"),
        ("2023-05-05", "Buddha Purnima"),
        ("2023-06-29", "Bakri Id"),
        ("2023-07-29", "Muharram"),
        ("2023-08-15", "Independence Day"),
        ("2023-09-07", "Janmashtami"),
        ("2023-09-19", "Ganesh Chaturthi"),
        ("2023-10-02", "Gandhi Jayanti"),
        ("2023-10-24", "Dussehra"),
        ("2023-11-12", "Diwali"),
        ("2023-11-14", "Diwali-Balipratipada"),
        ("2023-11-27", "Guru Nanak Jayanti"),
        ("2023-12-25", "Christmas"),
    ],
    2024: [
        ("2024-01-26", "Republic Day"),
        ("2024-03-08", "Maha Shivratri"),
        ("2024-03-25", "Holi"),
        ("2024-03-29", "Good Friday"),
        ("2024-04-11", "Id-Ul-Fitr (Ramadan Eid)"),
        ("2024-04-17", "Ram Navami"),
        ("2024-04-21", "Mahavir Jayanti"),
        ("2024-05-01", "Maharashtra Day"),
        ("2024-05-23", "Buddha Purnima"),
        ("2024-06-17", "Bakri Id"),
        ("2024-07-17", "Muharram"),
        ("2024-08-15", "Independence Day"),
        ("2024-08-26", "Janmashtami"),
        ("2024-09-16", "Ganesh Chaturthi"),
        ("2024-10-02", "Gandhi Jayanti"),
        ("2024-10-12", "Dussehra"),
        ("2024-11-01", "Diwali-Laxmi Pujan"),
        ("2024-11-15", "Guru Nanak Jayanti"),
        ("2024-12-25", "Christmas"),
    ],
}


class IndianStockHolidayScraper:
    """
    Smart Holiday Scraper with Pickle Storage
    - Automatically loads historical data from pickle
    - Fetches current year from live APIs
    - Updates pickle file with new data
    - Exports to Excel based on date range
    """
    
    def __init__(self, pickle_file: str = "indian_stock_holidays.pkl"):
        self.nse_base_url = "https://www.nseindia.com"
        self.bse_base_url = "https://www.bseindia.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.current_year = datetime.now().year
        
        # Create data directory if it doesn't exist
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)  # One level up to reach project root
        cache_dir = os.path.join(project_root, 'data', 'cache_files')
        os.makedirs(cache_dir, exist_ok=True)
        
        # Set pickle file path to data/cache_files folder
        self.pickle_file = os.path.join(cache_dir, pickle_file)
        
        # Load or initialize holiday database
        self.holidays_db = self._load_or_initialize_db()
    
    def _load_or_initialize_db(self) -> Dict:
        """Load existing pickle file or create new one with historical data"""
        if os.path.exists(self.pickle_file):
            print(f"Loading existing holiday database from {self.pickle_file}...")
            try:
                with open(self.pickle_file, 'rb') as f:
                    db = pickle.load(f)
                print(f"✓ Loaded {len(db)} years of holiday data")
                return db
            except Exception as e:
                print(f"Error loading pickle file: {e}")
                print("Creating new database...")
        
        print("Initializing new holiday database with historical data...")
        db = self._convert_historical_data()
        self._save_db(db)
        print(f"✓ Created new database with {len(db)} years")
        return db
    
    def _convert_historical_data(self) -> Dict:
        """Convert HISTORICAL_HOLIDAYS_DATA to standardized format"""
        db = {}
        for year, holidays in HISTORICAL_HOLIDAYS_DATA.items():
            db[year] = []
            for date_str, holiday_name in holidays:
                try:
                    dt = datetime.strptime(date_str, '%Y-%m-%d')
                    # Only include weekdays
                    if dt.weekday() < 5:
                        db[year].append({
                            'date': date_str,
                            'holiday': holiday_name,
                            'day': dt.strftime('%A'),
                            'source': 'Historical',
                            'year': year
                        })
                except ValueError:
                    continue
        return db
    
    def _save_db(self, db: Dict = None):
        """Save holiday database to pickle file"""
        if db is None:
            db = self.holidays_db
        
        try:
            with open(self.pickle_file, 'wb') as f:
                pickle.dump(db, f)
            print(f"✓ Database saved to {self.pickle_file}")
        except Exception as e:
            print(f"Error saving database: {e}")
    
    def _fetch_live_holidays(self) -> List[Dict]:
        """Fetch current year holidays from NSE/BSE APIs"""
        print(f"\nFetching live data for year {self.current_year}...")
        
        all_holidays = []
        
        # Try NSE
        print("  Attempting NSE...")
        try:
            self.session.get(f"{self.nse_base_url}/", timeout=10)
            
            api_endpoints = [
                f"{self.nse_base_url}/api/holiday-master?type=trading",
                f"{self.nse_base_url}/api/holiday-master",
            ]
            
            for endpoint in api_endpoints:
                try:
                    response = self.session.get(endpoint, timeout=15)
                    if response.status_code == 200:
                        data = response.json()
                        holidays = self._parse_nse_json_response(data)
                        if holidays:
                            all_holidays.extend(holidays)
                            print(f"  ✓ Found {len(holidays)} holidays from NSE")
                            break
                except:
                    continue
        except Exception as e:
            print(f"  ✗ NSE failed: {e}")
        
        # Try BSE
        print("  Attempting BSE...")
        try:
            bse_urls = [
                f"{self.bse_base_url}/markets/marketinfo/listholi.aspx",
            ]
            
            for url in bse_urls:
                try:
                    response = self.session.get(url, timeout=15)
                    if response.status_code == 200:
                        holidays = self._parse_bse_html_response(response.text)
                        if holidays:
                            all_holidays.extend(holidays)
                            print(f"  ✓ Found {len(holidays)} holidays from BSE")
                            break
                except:
                    continue
        except Exception as e:
            print(f"  ✗ BSE failed: {e}")
        
        # Remove duplicates
        unique_holidays = {}
        for holiday in all_holidays:
            date_key = holiday['date']
            if date_key not in unique_holidays:
                unique_holidays[date_key] = holiday
            else:
                # Prefer NSE over BSE
                if holiday['source'] == 'NSE':
                    unique_holidays[date_key] = holiday
        
        return list(unique_holidays.values())
    
    def _parse_nse_json_response(self, data: Dict) -> List[Dict]:
        """Parse NSE JSON API response"""
        holidays = []
        
        try:
            holiday_data = None
            
            if isinstance(data, dict):
                for key in ['trading', 'holidays', 'CM', 'FO', 'CD']:
                    if key in data:
                        holiday_data = data[key]
                        break
            elif isinstance(data, list):
                holiday_data = data
            
            if holiday_data:
                for item in holiday_data:
                    if isinstance(item, dict):
                        holiday_date = item.get('tradingDate') or item.get('date') or item.get('holidayDate')
                        holiday_desc = item.get('description') or item.get('occasion') or item.get('holiday')
                        
                        if holiday_date and holiday_desc:
                            parsed_date = self._parse_date_string(holiday_date)
                            if parsed_date and parsed_date.weekday() < 5:
                                holidays.append({
                                    'date': parsed_date.strftime('%Y-%m-%d'),
                                    'holiday': holiday_desc.strip(),
                                    'day': parsed_date.strftime('%A'),
                                    'source': 'NSE',
                                    'year': parsed_date.year
                                })
        except Exception as e:
            print(f"Error parsing NSE JSON: {e}")
        
        return holidays
    
    def _parse_bse_html_response(self, html: str) -> List[Dict]:
        """Parse BSE HTML response"""
        holidays = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                
                for row in rows[1:]:
                    cells = row.find_all(['td', 'th'])
                    
                    if len(cells) >= 2:
                        date_text = cells[0].get_text(strip=True)
                        holiday_text = cells[1].get_text(strip=True)
                        
                        if date_text and holiday_text and date_text.lower() != 'date':
                            parsed_date = self._parse_date_string(date_text)
                            if parsed_date and parsed_date.weekday() < 5:
                                holidays.append({
                                    'date': parsed_date.strftime('%Y-%m-%d'),
                                    'holiday': holiday_text.strip(),
                                    'day': parsed_date.strftime('%A'),
                                    'source': 'BSE',
                                    'year': parsed_date.year
                                })
        except Exception as e:
            print(f"Error parsing BSE HTML: {e}")
        
        return holidays
    
    def _parse_date_string(self, date_str: str) -> Optional[datetime]:
        """Parse date string in various formats"""
        date_str = re.sub(r'[^\w\s\-/.]', '', date_str.strip())
        
        date_formats = [
            '%d-%m-%Y', '%d/%m/%Y', '%Y-%m-%d', '%m/%d/%Y',
            '%d %b %Y', '%d %B %Y', '%b %d, %Y', '%B %d, %Y',
            '%d-%b-%Y', '%d-%B-%Y', '%Y/%m/%d', '%d.%m.%Y'
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        return None
    
    def update_current_year(self, force: bool = False):
        """
        Update database with current year data from live APIs
        
        Args:
            force: If True, fetch even if current year already exists in DB
        """
        if self.current_year in self.holidays_db and not force:
            print(f"\n⚠ Year {self.current_year} already exists in database.")
            print("  Use force=True to refresh the data.")
            return
        
        print(f"\n{'='*60}")
        print(f"Updating database with {self.current_year} holidays...")
        print(f"{'='*60}")
        
        live_holidays = self._fetch_live_holidays()
        
        if live_holidays:
            self.holidays_db[self.current_year] = live_holidays
            self._save_db()
            print(f"\n✓ Added {len(live_holidays)} holidays for year {self.current_year}")
        else:
            print(f"\n✗ No live data found for year {self.current_year}")
    
    def get_holidays(self, start_date: str = None, end_date: str = None) -> List[Dict]:
        """
        Get holidays from pickle database for given date range
        
        Args:
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
        
        Returns:
            List of holiday dictionaries sorted by date
        """
        if not start_date:
            start_date = f"{self.current_year}-01-01"
        if not end_date:
            end_date = f"{self.current_year}-12-31"
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        print(f"\n{'='*60}")
        print(f"Retrieving holidays from {start_date} to {end_date}")
        print(f"{'='*60}")
        
        all_holidays = []
        
        # Collect holidays from all relevant years
        for year in range(start_dt.year, end_dt.year + 1):
            if year in self.holidays_db:
                year_holidays = self.holidays_db[year]
                
                # Filter by date range
                for holiday in year_holidays:
                    try:
                        hol_dt = datetime.strptime(holiday['date'], '%Y-%m-%d')
                        if start_dt <= hol_dt <= end_dt:
                            all_holidays.append(holiday)
                    except:
                        continue
                
                print(f"  Year {year}: {len([h for h in year_holidays if start_dt <= datetime.strptime(h['date'], '%Y-%m-%d') <= end_dt])} holidays")
            else:
                print(f"  Year {year}: ⚠ NOT IN DATABASE")
                if year == self.current_year:
                    print(f"    → Run update_current_year() to fetch {year} data")
        
        # Sort by date
        all_holidays.sort(key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d'))
        
        print(f"\n{'='*60}")
        print(f"TOTAL HOLIDAYS FOUND: {len(all_holidays)}")
        print(f"{'='*60}\n")
        
        return all_holidays
    
    def get_holidays_dataframe(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """Get holidays as pandas DataFrame"""
        holidays = self.get_holidays(start_date, end_date)
        
        if holidays:
            df = pd.DataFrame(holidays)
            df['date'] = pd.to_datetime(df['date'])
            return df
        else:
            return pd.DataFrame(columns=['date', 'holiday', 'day', 'source', 'year'])
    
    def export_to_excel(self, filename: str, start_date: str = None, end_date: str = None):
        """
        Export holidays to Excel file
        
        Args:
            filename: Output Excel filename (should end with .xlsx)
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
        """
        df = self.get_holidays_dataframe(start_date, end_date)
        
        if not df.empty:
            # Ensure filename has .xlsx extension
            if not filename.endswith('.xlsx'):
                filename = filename.rsplit('.', 1)[0] + '.xlsx'
            
            # Create Excel writer with formatting
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Stock Market Holidays', index=False)
                
                # Auto-adjust column widths
                worksheet = writer.sheets['Stock Market Holidays']
                for idx, col in enumerate(df.columns):
                    max_length = max(
                        df[col].astype(str).apply(len).max(),
                        len(col)
                    ) + 2
                    worksheet.column_dimensions[chr(65 + idx)].width = max_length
            
            print(f"\n✓ Exported {len(df)} holidays to {filename}")
            print(f"  Date range: {df['date'].min().date()} to {df['date'].max().date()}")
        else:
            print("\n✗ No holidays found for the specified date range")
    
    def export_to_csv(self, filename: str, start_date: str = None, end_date: str = None):
        """
        Export holidays to CSV file
        
        Args:
            filename: Output CSV filename
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
        """
        df = self.get_holidays_dataframe(start_date, end_date)
        
        if not df.empty:
            df.to_csv(filename, index=False)
            print(f"\n✓ Exported {len(df)} holidays to {filename}")
        else:
            print("\n✗ No holidays found for the specified date range")
    
    def show_database_stats(self):
        """Display statistics about the holiday database"""
        print(f"\n{'='*60}")
        print("DATABASE STATISTICS")
        print(f"{'='*60}")
        print(f"Pickle file: {self.pickle_file}")
        print(f"Total years in database: {len(self.holidays_db)}")
        print(f"Year range: {min(self.holidays_db.keys())} - {max(self.holidays_db.keys())}")
        
        total_holidays = sum(len(holidays) for holidays in self.holidays_db.values())
        print(f"Total holidays: {total_holidays}")
        
        print(f"\nYear-wise breakdown:")
        for year in sorted(self.holidays_db.keys()):
            holidays = self.holidays_db[year]
            sources = set(h['source'] for h in holidays)
            print(f"  {year}: {len(holidays):2d} holidays (Sources: {', '.join(sources)})")
        
        print(f"{'='*60}\n")


def main():
    """
    Example usage showing all features
    """
    # Initialize scraper (will load or create pickle file)
    scraper = IndianStockHolidayScraper(pickle_file="indian_stock_holidays.pkl")
    
    # Show database statistics
    scraper.show_database_stats()
    
    # Update with current year data (if not already present)
    scraper.update_current_year()
    
    # Full path to the Output Directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)  # One level up to reach project root
    excel_dir = os.path.join(project_root, 'output', 'excel_files')
    os.makedirs(excel_dir, exist_ok=True)

    
    # Example 1: Export wide range to Excel
    current_year = datetime.now().year
    print("\n" + "="*60)
    print("EXAMPLE 1: Export HSensex Holidays to Excel")
    print("="*60)   
    excel_file_path = os.path.join(excel_dir, 'Sensex_Holidays.xlsx')
    scraper.export_to_excel(
        excel_file_path,
        start_date='2000-01-01',
        end_date=f'{current_year}-12-31'
    )
    
    """
    # Example 2: Get specific year range as DataFrame
    print("\n" + "="*60)
    print("EXAMPLE 2: Get 2023-2024 holidays as DataFrame")
    print("="*60)
    df = scraper.get_holidays_dataframe('2023-01-01', '2024-12-31')
    print(df.head(10))
    
    # Example 3: Export to CSV
    print("\n" + "="*60)
    print("EXAMPLE 3: Export 2024 holidays to CSV")
    print("="*60)
    csv_file_path = os.path.join(excel_dir, 'Holidays_2024.csv')
    scraper.export_to_csv(
        csv_file_path,
        start_date='2024-01-01',
        end_date='2024-12-31'
    )
    """
    
    # Show final statistics
    scraper.show_database_stats()


if __name__ == "__main__":
    main()