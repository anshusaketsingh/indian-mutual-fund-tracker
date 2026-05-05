#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 14 13:55:46 2025

@author: anshusaketsingh
"""

import pandas as pd
from mftool import Mftool
from datetime import datetime, timedelta
import calendar
import time
import warnings
warnings.filterwarnings('ignore')

# Concurrent processing imports
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import os
import json
import pickle
from pathlib import Path
import hashlib

# Tableau Hyper API imports
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, escape_name, escape_string_literal, \
    HyperException, TableName

class OptimizedMutualFundExtractor:
    def __init__(self, max_workers=10, api_delay=0.1, cache_dir="nav_cache"):
        """
        Initialize with concurrency controls and caching
        
        Args:
            max_workers: Number of concurrent threads (default: 10)
            api_delay: Delay between API calls in seconds (default: 0.1)
            cache_dir: Directory to store NAV cache files (default: "nav_cache")
        """
        self.mf = Mftool()
        self.max_workers = max_workers
        self.api_delay = api_delay
        self.rate_limiter = threading.Semaphore(max_workers)
        self.results_lock = threading.Lock()
        
        # Get the directory where the Python script is located
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(self.script_dir)  # One level up to reach project root
        
        # Cache management - use data folder
        cache_dir = os.path.join(project_root, 'data', cache_dir)
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True, parents=True)
        self.cache_lock = threading.Lock()
        
        # Cache file paths
        self.nav_cache_file = self.cache_dir / "nav_data_cache.pkl"
        self.cache_metadata_file = self.cache_dir / "cache_metadata.json"
        self.metadata_cache_file = self.cache_dir / "scheme_metadata_cache.pkl"
        
        # Load existing cache
        self.nav_cache = self._load_nav_cache()
        self.metadata_cache = self._load_metadata_cache()
        self._reset_cache_stats()
        
    def _reset_cache_stats(self):
        """
        Reset cache statistics with all required keys
        """
        self.cache_stats = {
            "hits": 0, 
            "misses": 0, 
            "new_data": 0, 
            "metadata_hits": 0, 
            "metadata_misses": 0
        }

    def _ensure_cache_stats_initialized(self):
        """
        Ensure all required cache stats keys exist
        """
        required_keys = ["hits", "misses", "new_data", "metadata_hits", "metadata_misses"]
        for key in required_keys:
            if key not in self.cache_stats:
                self.cache_stats[key] = 0

    def _get_default_date_range(self):
        """
        Get default date range (current year-to-date)
        Returns: (start_date, end_date) as datetime objects
        """
        current_date = datetime.now()
        start_date = datetime(current_date.year, 1, 1)  # January 1st of current year
        end_date = current_date  # Today
        return start_date, end_date
    
    def _parse_date_input(self, date_input):
        """
        Parse date input which can be:
        - datetime object
        - string in format 'YYYY-MM-DD' or 'YYYY'
        - None (returns None)
        """
        if date_input is None:
            return None
            
        if isinstance(date_input, datetime):
            return date_input
            
        if isinstance(date_input, str):
            try:
                # Try parsing as YYYY-MM-DD
                if len(date_input) == 10 and '-' in date_input:
                    return datetime.strptime(date_input, '%Y-%m-%d')
                # Try parsing as YYYY (assume January 1st)
                elif len(date_input) == 4 and date_input.isdigit():
                    return datetime(int(date_input), 1, 1)
                else:
                    # Try generic parsing
                    return pd.to_datetime(date_input).to_pydatetime()
            except:
                raise ValueError(f"Invalid date format: {date_input}. Use 'YYYY-MM-DD' or 'YYYY'")
        
        raise ValueError(f"Invalid date input type: {type(date_input)}")
    
    def _validate_date_range(self, start_date, end_date):
        """
        Validate and adjust date range
        """
        if start_date and end_date and start_date > end_date:
            raise ValueError(f"Start date ({start_date.date()}) cannot be after end date ({end_date.date()})")
        
        # Don't allow future dates beyond today
        today = datetime.now().date()
        if end_date and end_date.date() > today:
            print(f"Warning: End date adjusted from {end_date.date()} to {today} (cannot fetch future data)")
            end_date = datetime.combine(today, datetime.min.time())
        
        return start_date, end_date
    
        """
        PERSISTENT CACHING APPROACH:
        This extractor uses a dual persistent cache system that NEVER clears historical data.
        Since mutual fund NAV values and scheme metadata are historical records that never change, the cache:
        - Accumulates NAV data over time with each execution
        - Stores scheme metadata permanently (fund house, category, start date, etc.)
        - Never expires or deletes old records

        Cache files are stored in the 'nav_cache' directory and persist between executions:
        - nav_data_cache.pkl: Historical NAV data (grows over time)
        - scheme_metadata_cache.pkl: Scheme details and metadata (permanent)
        - cache_metadata.json: Cache statistics and policy information
        """
    
    def _load_nav_cache(self):
        """
        Load existing NAV cache from disk
        Cache structure: {scheme_code: {date: nav_value, ...}, ...}
        NOTE: Cache is persistent and never cleared since mutual fund historical data never changes
        """
        try:
            if self.nav_cache_file.exists():
                with open(self.nav_cache_file, 'rb') as f:
                    cache = pickle.load(f)
                
                # Load metadata if available
                if self.cache_metadata_file.exists():
                    with open(self.cache_metadata_file, 'r') as f:
                        metadata = json.load(f)
                    print(f"Loaded NAV cache: {len(cache)} schemes, last updated: {metadata.get('last_updated', 'Unknown')}")
                else:
                    print(f"Loaded NAV cache: {len(cache)} schemes")
                return cache
            else:
                print("No existing NAV cache found. Starting fresh.")
                return {}
        except Exception as e:
            print(f"Error loading cache: {e}. Starting with empty cache.")
            return {}

    def _load_metadata_cache(self):
        """
        Load existing metadata cache from disk
        Cache structure: {scheme_code: {scheme_details, nav_quote, processed_metadata}}
        """
        try:
            if self.metadata_cache_file.exists():
                with open(self.metadata_cache_file, 'rb') as f:
                    cache = pickle.load(f)
                print(f"Loaded metadata cache: {len(cache)} schemes")
                return cache
            else:
                print("No existing metadata cache found. Starting fresh.")
                return {}
        except Exception as e:
            print(f"Error loading metadata cache: {e}. Starting with empty cache.")
            return {}

    def _save_nav_cache(self):
        """
        Save NAV cache to disk
        NOTE: Cache is persistent and accumulates data over time - never cleared
        """
        try:
            with self.cache_lock:
                # Save cache data
                with open(self.nav_cache_file, 'wb') as f:
                    pickle.dump(self.nav_cache, f)
                
                # Save metadata
                metadata = {
                    "last_updated": datetime.now().isoformat(),
                    "total_schemes": len(self.nav_cache),
                    "total_records": sum(len(dates) for dates in self.nav_cache.values()),
                    "cache_policy": "persistent_accumulative"
                }
                
                with open(self.cache_metadata_file, 'w') as f:
                    json.dump(metadata, f, indent=2)
                
                print(f"Cache saved: {metadata['total_schemes']} schemes, {metadata['total_records']} records")
        except Exception as e:
            print(f"Warning: Error saving cache: {e}")

    def _save_metadata_cache(self):
        """
        Save metadata cache to disk
        NOTE: Metadata cache is persistent and accumulates data over time - never cleared
        """
        try:
            with self.cache_lock:
                # Save metadata cache data
                with open(self.metadata_cache_file, 'wb') as f:
                    pickle.dump(self.metadata_cache, f)
                
                print(f"Metadata cache saved: {len(self.metadata_cache)} schemes")
        except Exception as e:
            print(f"Warning: Error saving metadata cache: {e}")

    def _get_cached_nav_data(self, scheme_code, start_date, end_date):
        """
        Get cached NAV data for a scheme within date range
        Returns: (cached_records, missing_date_range)
        """
        cached_records = []
        
        if scheme_code in self.nav_cache:
            scheme_cache = self.nav_cache[scheme_code]
            
            for date_str, nav_value in scheme_cache.items():
                try:
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                    # Check if date is within range
                    if start_date <= date_obj <= end_date:
                        cached_records.append({
                            'Scheme_Code': scheme_code,
                            'Date': date_str,
                            'NAV': nav_value,
                            'Year': date_obj.year,
                            'Month': date_obj.month,
                            'Day': date_obj.day,
                            'Weekday': date_obj.strftime('%A')
                        })
                except ValueError:
                    continue
            
            if cached_records:
                self._increment_cache_stat('hits')
                # Sort by date
                cached_records.sort(key=lambda x: x['Date'])
                
                # Check if we have all data for the requested range
                cached_dates = set(record['Date'] for record in cached_records)
                
                # Generate expected date range (business days approximation)
                expected_dates = pd.date_range(start=start_date, end=end_date, freq='D')
                expected_date_strings = set(date.strftime('%Y-%m-%d') for date in expected_dates)
                
                missing_dates = expected_date_strings - cached_dates
                
                # If we have recent data (within last 3 days) and not too many missing dates
                if cached_records:
                    latest_cached_date = datetime.strptime(cached_records[-1]['Date'], '%Y-%m-%d')
                    days_old = (datetime.now() - latest_cached_date).days
                    
                    if days_old <= 3 and len(missing_dates) < 10:
                        return cached_records, None  # Use cached data
                
                # Need to fetch missing data
                return cached_records, (start_date, end_date)
            else:
                # Have cache for scheme but no data in range
                self._increment_cache_stat('misses')
                return [], (start_date, end_date)
        else:
            # No cache for this scheme
            self._increment_cache_stat('misses')
            return [], (start_date, end_date)

    def _get_cached_metadata(self, scheme_code):
        """
        Get cached metadata for a scheme
        Returns: cached metadata dict or None if not found
        """
        if scheme_code in self.metadata_cache:
            self._increment_cache_stat('metadata_hits')
            return self.metadata_cache[scheme_code]
        else:
            self._increment_cache_stat('metadata_misses')
            return None

    def _update_nav_cache(self, scheme_code, nav_records):
        """
        Update cache with new NAV data
        """
        if not nav_records:
            return
            
        with self.cache_lock:
            if scheme_code not in self.nav_cache:
                self.nav_cache[scheme_code] = {}
            
            # Add new data to existing cache (never overwrite)
            for record in nav_records:
                date_str = record['Date']
                nav_value = record['NAV']
                
                # Only add if not already in cache (preserve existing data)
                if date_str not in self.nav_cache[scheme_code]:
                    self.nav_cache[scheme_code][date_str] = nav_value
                    self._increment_cache_stat('new_data')

    def _update_metadata_cache(self, scheme_code, scheme_details, nav_quote, processed_metadata):
        """
        Update metadata cache with new scheme information
        NOTE: Metadata is never overwritten - only added if not exists
        """
        with self.cache_lock:
            if scheme_code not in self.metadata_cache:
                self.metadata_cache[scheme_code] = {}
            
            # Store all metadata components
            self.metadata_cache[scheme_code] = {
                'scheme_details': scheme_details,
                'nav_quote': nav_quote,
                'processed_metadata': processed_metadata,
                'cached_at': datetime.now().isoformat()
            }

    def _get_cache_stat(self, key, default=0):
        """
        Safely get cache stat value, returning default if key doesn't exist
        """
        return self.cache_stats.get(key, default)

    def _increment_cache_stat(self, key, increment=1):
        """
        Safely increment cache stat value, creating key if it doesn't exist
        """
        if key not in self.cache_stats:
            self.cache_stats[key] = 0
        self.cache_stats[key] += increment

    def get_cache_stats(self):
        """
        Display cache statistics
        """
        # Ensure all cache stats keys exist
        self._ensure_cache_stats_initialized()
        
        print("\n" + "="*50)
        print("NAV CACHE STATISTICS")
        print("="*50)
        
        total_schemes = len(self.nav_cache)
        total_records = sum(len(dates) for dates in self.nav_cache.values())
        
        print(f"Cached schemes: {total_schemes}")
        print(f"Cached records: {total_records}")
        print(f"Cache hits: {self._get_cache_stat('hits')}")
        print(f"Cache misses: {self._get_cache_stat('misses')}")
        print(f"New data fetched: {self._get_cache_stat('new_data')}")
        
        # Show metadata cache stats
        total_metadata_schemes = len(self.metadata_cache)
        print(f"\n METADATA CACHE:")
        print(f"   • Cached schemes: {total_metadata_schemes}")
        print(f"   • Metadata hits: {self._get_cache_stat('metadata_hits')}")
        print(f"   • Metadata misses: {self._get_cache_stat('metadata_misses')}")
        
        if self._get_cache_stat('metadata_hits') + self._get_cache_stat('metadata_misses') > 0:
            metadata_hit_rate = (self._get_cache_stat('metadata_hits') / (self._get_cache_stat('metadata_hits') + self._get_cache_stat('metadata_misses'))) * 100
            print(f"   • Metadata hit rate: {metadata_hit_rate:.1f}%")
        
        if self._get_cache_stat('hits') + self._get_cache_stat('misses') > 0:
            hit_rate = (self._get_cache_stat('hits') / (self._get_cache_stat('hits') + self._get_cache_stat('misses'))) * 100
            print(f" Cache hit rate: {hit_rate:.1f}%")
        
        
        # Show cache file info
        if self.nav_cache_file.exists():
            cache_size = self.nav_cache_file.stat().st_size / (1024 * 1024)  # MB
            print(f"\n Cache file size: {cache_size:.2f} MB")
            
            if self.cache_metadata_file.exists():
                try:
                    with open(self.cache_metadata_file, 'r') as f:
                        metadata = json.load(f)
                    last_updated = metadata.get('last_updated', 'Unknown')
                    print(f" Last updated: {last_updated}")
                except:
                    pass

    def show_cache_growth_benefits(self):
        """
        Display information about how persistent caching improves performance over time
        """
        # Ensure all cache stats keys exist
        self._ensure_cache_stats_initialized()
        
        print("\n" + "="*60)
        print("PERSISTENT CACHE PERFORMANCE BENEFITS")
        print("="*60)
        
        total_schemes = len(self.nav_cache)
        total_records = sum(len(dates) for dates in self.nav_cache.values())
        
        print(f"Current Cache Status:")
        print(f"   • Total schemes cached: {total_schemes}")
        print(f"   • Total NAV records: {total_records:,}")
        
        # Show metadata cache status
        total_metadata_schemes = len(self.metadata_cache)
        print(f"   • Metadata schemes cached: {total_metadata_schemes}")
        
        if total_records > 0:
            avg_records_per_scheme = total_records / total_schemes if total_schemes > 0 else 0
            print(f"   • Average records per scheme: {avg_records_per_scheme:.1f}")

    def check_cache_health(self):
        """
        Check cache health and show data reuse statistics
        """
        # Ensure all cache stats keys exist
        self._ensure_cache_stats_initialized()
        
        print("\n" + "="*50)
        print("CACHE HEALTH CHECK")
        print("="*50)
        
        if not self.nav_cache:
            print("Cache is empty - first run or cache not loaded")
            return
        
        total_schemes = len(self.nav_cache)
        total_records = sum(len(dates) for dates in self.nav_cache.values())
        
        # Calculate cache coverage
        if total_records > 0:
            # Estimate total possible records (assuming daily data for schemes)
            # This is a rough estimate for demonstration
            estimated_total_possible = total_schemes * 365  # Assuming 1 year of daily data
            coverage_percentage = min((total_records / estimated_total_possible) * 100, 100)
            
            print(f" NAV Cache Coverage:")
            print(f"   • Total schemes: {total_schemes}")
            print(f"   • Total records: {total_records:,}")
            print(f"   • Estimated coverage: {coverage_percentage:.1f}%")
        
        # Show metadata cache coverage
        total_metadata_schemes = len(self.metadata_cache)
        print(f"\n Metadata Cache Coverage:")
        print(f"   • Total schemes: {total_metadata_schemes}")
        print(f"   • Coverage: {total_metadata_schemes} schemes cached")
        
        # Show overall cache efficiency
        total_requests = self._get_cache_stat('hits') + self._get_cache_stat('misses')
        total_metadata_requests = self._get_cache_stat('metadata_hits') + self._get_cache_stat('metadata_misses')
        
        if total_requests > 0 or total_metadata_requests > 0:
            print(f"\n Overall Cache Efficiency:")
            
            if total_requests > 0:
                nav_efficiency = (self._get_cache_stat('hits') / total_requests) * 100
                print(f"   • NAV cache hit rate: {nav_efficiency:.1f}%")
            
            if total_metadata_requests > 0:
                metadata_efficiency = (self._get_cache_stat('metadata_hits') / total_metadata_requests) * 100
                print(f"   • Metadata cache hit rate: {metadata_efficiency:.1f}%")
            
            # Overall efficiency
            total_overall_requests = total_requests + total_metadata_requests
            total_overall_hits = self._get_cache_stat('hits') + self._get_cache_stat('metadata_hits')
            if total_overall_requests > 0:
                overall_efficiency = (total_overall_hits / total_overall_requests) * 100
                print(f"   • Overall cache efficiency: {overall_efficiency:.1f}%")
                
        
        # Show data freshness
        if self.cache_metadata_file.exists():
            try:
                with open(self.cache_metadata_file, 'r') as f:
                    metadata = json.load(f)
                last_updated = metadata.get('last_updated', 'Unknown')
                print(f"\n Cache Status:")
                print(f"   • Last updated: {last_updated}")
                print(f"   • Cache policy: {metadata.get('cache_policy', 'Unknown')}")
            except:
                pass
        


    def _cleanup_log_files(self):
        """
        Clean up all .log files created during execution
        """
        try:
            import glob
            import os
            
            # Find all .log files in current directory and subdirectories
            log_files = glob.glob("*.log") + glob.glob("**/*.log", recursive=True)
            
            if log_files:
                cleaned_count = 0
                for log_file in log_files:
                    try:
                        os.remove(log_file)
                        cleaned_count += 1
                    except Exception as e:
                        continue  # Skip files that can't be removed
                
                if cleaned_count > 0:
                    print(f"Cleaned up {cleaned_count} log files")
            else:
                print("No log files found to clean up")
                
        except Exception as e:
            print(f"Log cleanup warning: {e}")

    def _rate_limited_api_call(self, func, *args, **kwargs):
        """
        Rate-limited API call wrapper
        """
        with self.rate_limiter:
            try:
                result = func(*args, **kwargs)
                time.sleep(self.api_delay)
                return result
            except Exception as e:
                return None
    
    @lru_cache(maxsize=1000)
    def _cached_scheme_details(self, scheme_code):
        """
        Cached scheme details to avoid duplicate API calls
        """
        return self._rate_limited_api_call(self.mf.get_scheme_details, scheme_code)
    
    @lru_cache(maxsize=1000)
    def _cached_scheme_quote(self, scheme_code):
        """
        Cached scheme quote to avoid duplicate API calls
        """
        return self._rate_limited_api_call(self.mf.get_scheme_quote, scheme_code)
        
    def _normalize_fund_house_filter(self, fund_house_filter):
        """Normalize fund house filter to list format"""
        if fund_house_filter is None:
            return None
        elif isinstance(fund_house_filter, str):
            if fund_house_filter.lower() in ['all', 'none']:
                return None
            return [fund_house_filter]
        elif isinstance(fund_house_filter, list):
            return fund_house_filter
        else:
            return [str(fund_house_filter)]
    
    def _normalize_category_filter(self, category_filter):
        """Normalize category filter to list format"""
        if category_filter is None or (isinstance(category_filter, str) and category_filter.lower() in ['all', 'none']):
            return None
        elif isinstance(category_filter, str):
            if ',' in category_filter:
                return [cat.strip() for cat in category_filter.split(',')]
            return [category_filter]
        elif isinstance(category_filter, list):
            return category_filter
        else:
            return [str(category_filter)]
    
    def _calculate_eta(self, current_index, total_items, start_time):
        """Calculate estimated time remaining"""
        if current_index == 0:
            return "Calculating..."
        
        elapsed_time = time.time() - start_time
        avg_time_per_item = elapsed_time / current_index
        remaining_items = total_items - current_index
        eta_seconds = remaining_items * avg_time_per_item
        
        if eta_seconds < 60:
            return f"{int(eta_seconds)}s"
        elif eta_seconds < 3600:
            minutes = int(eta_seconds // 60)
            seconds = int(eta_seconds % 60)
            return f"{minutes}m {seconds}s"
        else:
            hours = int(eta_seconds // 3600)
            minutes = int((eta_seconds % 3600) // 60)
            return f"{hours}h {minutes}m"
    
    def _process_single_scheme_metadata(self, scheme_info):
        """
        Process metadata for a single scheme (thread-safe)
        """
        code, name = scheme_info
        try:
            # First check persistent metadata cache
            cached_metadata = self._get_cached_metadata(code)
            if cached_metadata:
                # Return cached processed metadata
                return cached_metadata['processed_metadata']
            
            # If not in cache, fetch from API
            scheme_data = self._cached_scheme_details(code)
            
            if not scheme_data:
                return None
                
            # Get current NAV using cached method
            nav_data = self._cached_scheme_quote(code)
            current_nav = nav_data.get('nav', None) if nav_data else None
            
            # Parse start date and NAV
            start_date_info = scheme_data.get('scheme_start_date', '')
            
            fund_info = {
                'Scheme_Code': code,
                'Scheme_Name': name,
                'Fund_House': scheme_data.get('fund_house', ''),
                'Scheme_Type': scheme_data.get('scheme_type', ''),
                'Scheme_Category': scheme_data.get('scheme_category', ''),
                'Scheme_Start_Date_Info': start_date_info,
                'Current_NAV': current_nav,
                'Last_Updated': nav_data.get('last_updated', '') if nav_data else '',
            }
            
            # Classify main category
            category = scheme_data.get('scheme_category', '').lower()
            if any(word in category for word in ['equity', 'growth', 'large cap', 'mid cap', 'small cap']):
                fund_info['Main_Category'] = 'Equity'
            elif any(word in category for word in ['debt', 'income', 'bond', 'gilt', 'corporate']):
                fund_info['Main_Category'] = 'Debt'
            elif any(word in category for word in ['hybrid', 'balanced', 'aggressive', 'conservative']):
                fund_info['Main_Category'] = 'Hybrid'
            elif any(word in category for word in ['liquid', 'money market', 'overnight']):
                fund_info['Main_Category'] = 'Liquid'
            elif any(word in category for word in ['index', 'etf']):
                fund_info['Main_Category'] = 'Index/ETF'
            else:
                fund_info['Main_Category'] = 'Others'
            
            # Cache the metadata for future use
            self._update_metadata_cache(code, scheme_data, nav_data, fund_info)
            
            return fund_info
            
        except Exception as e:
            print(f"  Error processing scheme {code}: {str(e)}")
            return None
    
    def get_mutual_fund_metadata(self, fund_house_filter=None, category_filter=None):
        """
        Fetch metadata of mutual funds with optimized concurrent processing
        """
        try:
            # Normalize filters
            fund_houses = self._normalize_fund_house_filter(fund_house_filter)
            categories = self._normalize_category_filter(category_filter)
            
            # Display filtering info
            if fund_houses:
                print(f"Fetching funds for: {', '.join(fund_houses)}")
            else:
                print("Fetching ALL mutual funds")
            
            if categories:
                print(f"Category filter: {', '.join(categories)}")
            else:
                print("All categories included")
            
            # Get all scheme codes
            print("Fetching scheme codes...")
            all_schemes = self.mf.get_scheme_codes()
            
            if not all_schemes:
                print("Failed to fetch scheme codes")
                return pd.DataFrame()
            
            print(f"Found {len(all_schemes)} total schemes in database")
            
            # Apply fund house filter first
            if fund_houses:
                filtered_schemes = {}
                for code, name in all_schemes.items():
                    name_lower = name.lower()
                    if any(fh.lower() in name_lower for fh in fund_houses):
                        filtered_schemes[code] = name
            else:
                filtered_schemes = all_schemes
            
            total_schemes = len(filtered_schemes)
            print(f"Processing {total_schemes} schemes with {self.max_workers} concurrent threads...")
            
            # Process schemes concurrently
            fund_data = []
            completed_count = 0
            start_time = time.time()
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all tasks
                future_to_scheme = {
                    executor.submit(self._process_single_scheme_metadata, (code, name)): (code, name)
                    for code, name in filtered_schemes.items()
                }
                
                # Process completed tasks
                for future in as_completed(future_to_scheme):
                    completed_count += 1
                    
                    # Progress tracking
                    if completed_count % 50 == 0 or completed_count == total_schemes:
                        eta = self._calculate_eta(completed_count, total_schemes, start_time)
                        print(f"Progress: {completed_count}/{total_schemes} | ETA: {eta}")
                    
                    try:
                        result = future.result()
                        if result:
                            # Apply category filter
                            if categories:
                                if not any(cat.lower() in result['Main_Category'].lower() for cat in categories):
                                    continue  # Skip this fund
                            
                            with self.results_lock:
                                fund_data.append(result)
                            
                            if completed_count % 100 == 0:
                                print(f"Processed: {result['Fund_House']} - {result['Main_Category']}")
                                
                    except Exception as e:
                        code, name = future_to_scheme[future]
                        print(f" Error processing scheme {code}: {str(e)}")
                        continue
            
            metadata_df = pd.DataFrame(fund_data)
            
            # Final filter summary
            filter_desc = []
            if fund_houses:
                filter_desc.append(f"Fund Houses: {', '.join(fund_houses)}")
            if categories:
                filter_desc.append(f"Categories: {', '.join(categories)}")
            
            filter_text = f" ({'; '.join(filter_desc)})" if filter_desc else " (All funds)"
            
            elapsed_time = time.time() - start_time
            print(f"\nSuccessfully fetched metadata for {len(metadata_df)} mutual funds{filter_text}")
            print(f"Total time: {elapsed_time:.1f} seconds")
            return metadata_df
            
        except Exception as e:
            print(f" Error fetching metadata: {str(e)}")
            return pd.DataFrame()
    
    def _process_single_scheme_nav(self, scheme_code, start_date, end_date):
        """
        Process NAV data for a single scheme with caching support and date range
        """
        # First, check cache
        cached_records, missing_date_range = self._get_cached_nav_data(scheme_code, start_date, end_date)
        
        # If we have complete cached data, return it
        if cached_records and missing_date_range is None:
            return cached_records
        
        # Need to fetch some or all data from API
        api_records = []
        
        try:
            # Get historical data with rate limiting
            historical_data = self._rate_limited_api_call(
                self.mf.get_scheme_historical_nav, 
                scheme_code, 
                as_Dataframe=True
            )
            
            if historical_data is None:
                # Try dictionary method as fallback
                dict_data = self._rate_limited_api_call(
                    self.mf.get_scheme_historical_nav, 
                    scheme_code
                )
                if dict_data and isinstance(dict_data, dict):
                    historical_data = pd.DataFrame(dict_data)
            
            if historical_data is not None and not historical_data.empty:
                # Handle date column detection
                date_col = None
                nav_col = None
                
                # Check if date is in index
                if historical_data.index.name and 'date' in historical_data.index.name.lower():
                    historical_data = historical_data.reset_index()
                    date_col = historical_data.columns[0]
                elif pd.api.types.is_datetime64_any_dtype(historical_data.index) or \
                     (len(historical_data.index) > 0 and isinstance(historical_data.index[0], str) and
                      any(char in str(historical_data.index[0]) for char in ['-', '/'])):
                    historical_data = historical_data.reset_index()
                    date_col = 'index'
                
                # Find date column in regular columns if not found yet
                if date_col is None:
                    for col in historical_data.columns:
                        if 'date' in col.lower():
                            date_col = col
                            break
                
                # Find NAV column
                for col in historical_data.columns:
                    if 'nav' in col.lower() and nav_col is None:
                        nav_col = col
                        break
                
                if date_col and nav_col:
                    # Convert dates
                    date_formats = ['%d-%m-%Y', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y']
                    date_converted = False
                    
                    for date_format in date_formats:
                        try:
                            historical_data[date_col] = pd.to_datetime(historical_data[date_col], format=date_format)
                            date_converted = True
                            break
                        except:
                            continue
                    
                    if not date_converted:
                        try:
                            historical_data[date_col] = pd.to_datetime(historical_data[date_col], infer_datetime_format=True)
                            date_converted = True
                        except:
                            # Return only cached data if API parsing fails
                            return cached_records
                    
                    if date_converted:
                        # Filter for the requested date range
                        filtered_data = historical_data[
                            (historical_data[date_col] >= start_date) & 
                            (historical_data[date_col] <= end_date)
                        ].copy()
                        
                        if not filtered_data.empty:
                            filtered_data = filtered_data.sort_values(date_col)
                            
                            # Process API records
                            for _, row in filtered_data.iterrows():
                                try:
                                    nav_value = float(row[nav_col])
                                    date_value = row[date_col]
                                    
                                    nav_entry = {
                                        'Scheme_Code': scheme_code,
                                        'Date': date_value.strftime('%Y-%m-%d'),
                                        'NAV': nav_value,
                                        'Year': date_value.year,
                                        'Month': date_value.month,
                                        'Day': date_value.day,
                                        'Weekday': date_value.strftime('%A')
                                    }
                                    api_records.append(nav_entry)
                                except (ValueError, TypeError):
                                    continue  # Skip invalid NAV values
                            
                            # Update cache with new data
                            if api_records:
                                self._update_nav_cache(scheme_code, api_records)
            
            # Combine cached and new API records, filter by date range
            all_records = cached_records + api_records
            
            # Filter records to ensure they're within the requested date range
            filtered_records = []
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            for record in all_records:
                if start_date_str <= record['Date'] <= end_date_str:
                    filtered_records.append(record)
            
            # Sort by date and remove duplicates
            if filtered_records:
                # Remove duplicates by date
                seen_dates = set()
                unique_records = []
                for record in sorted(filtered_records, key=lambda x: x['Date']):
                    if record['Date'] not in seen_dates:
                        unique_records.append(record)
                        seen_dates.add(record['Date'])
                
                return unique_records
            else:
                return []
            
        except Exception as e:
            # Return cached data if API call fails
            filtered_cached = []
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            for record in cached_records:
                if start_date_str <= record['Date'] <= end_date_str:
                    filtered_cached.append(record)
            
            return filtered_cached
    
    def fetch_daily_nav_data(self, scheme_codes, start_date=None, end_date=None):
        """
        Fetch DAILY NAV data with optimized caching and concurrent processing
        
        Args:
            scheme_codes: List of scheme codes to fetch data for
            start_date: Start date (datetime, 'YYYY-MM-DD', 'YYYY', or None for current YTD)
            end_date: End date (datetime, 'YYYY-MM-DD', 'YYYY', or None for today)
        """
        # Parse and validate dates
        if start_date is None and end_date is None:
            start_date, end_date = self._get_default_date_range()
            print(f"Using default date range (Current YTD): {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        else:
            if start_date is None:
                start_date, _ = self._get_default_date_range()
            else:
                start_date = self._parse_date_input(start_date)
            
            if end_date is None:
                end_date = datetime.now()
            else:
                end_date = self._parse_date_input(end_date)
        
        start_date, end_date = self._validate_date_range(start_date, end_date)
        
        print(f"\nFetching DAILY NAV data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} with caching enabled...")
        
        total_schemes = len(scheme_codes)
        start_time = time.time()
        
        # Reset cache stats for this run
        self._reset_cache_stats()
        
        print(f"Processing {total_schemes} schemes for daily NAV data...")
        
        all_nav_data = []
        completed_count = 0
        
        # Process schemes in batches to manage memory and API rate limits
        # Smaller batches with current optimized settings
        batch_size = min(50, self.max_workers * 3)
        
        for i in range(0, total_schemes, batch_size):
            batch_schemes = scheme_codes[i:i + batch_size]
            batch_nav_data = []
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit batch tasks
                future_to_scheme = {
                    executor.submit(self._process_single_scheme_nav, scheme_code, start_date, end_date): scheme_code
                    for scheme_code in batch_schemes
                }
                
                # Process completed tasks
                for future in as_completed(future_to_scheme):
                    completed_count += 1
                    scheme_code = future_to_scheme[future]
                    
                    # Progress tracking
                    if completed_count % 25 == 0 or completed_count == total_schemes:
                        eta = self._calculate_eta(completed_count, total_schemes, start_time)
                        hit_rate = (self._get_cache_stat("hits") / max(1, self._get_cache_stat("hits") + self._get_cache_stat("misses"))) * 100
                        print(f"NAV Progress: {completed_count}/{total_schemes} | ETA: {eta} | Cache Hit Rate: {hit_rate:.1f}%")
                    
                    try:
                        nav_records = future.result()
                        if nav_records:
                            batch_nav_data.extend(nav_records)
                            if completed_count % 1000 == 0:
                                cached_count = len([r for r in nav_records if r.get('_cached', False)])
                                api_count = len(nav_records) - cached_count
                                print(f"  Scheme {scheme_code}: {len(nav_records)} records ( API data)")
                    except Exception as e:
                        print(f" Error fetching NAV for scheme {scheme_code}: {str(e)}")
                        continue
            
            # Add batch results to main list
            all_nav_data.extend(batch_nav_data)
            
            # Memory management: Clear batch data
            del batch_nav_data
            
            # Progress update
            print(f"Batch {i//batch_size + 1} completed. Total records so far: {len(all_nav_data)}")
            
            # Save cache periodically (every 5 batches)
            if (i//batch_size + 1) % 5 == 0:
                print("Saving cache (periodic backup)...")
                self._save_nav_cache()
        
        # Final cache save
        print("Saving final cache...")
        self._save_nav_cache()
        
        nav_df = pd.DataFrame(all_nav_data)
        
        elapsed_time = time.time() - start_time
        print(f"\nSuccessfully fetched DAILY NAV data with {len(nav_df)} records")
        print(f"Total time: {elapsed_time:.1f} seconds")
        
        # Show cache performance
        self.get_cache_stats()
        
        if not nav_df.empty:
            print(f"\nDate range: {nav_df['Date'].min()} to {nav_df['Date'].max()}")
            
            # Data validation summary
            unique_schemes = nav_df['Scheme_Code'].nunique()
            print(f"Schemes with data: {unique_schemes}")
            if unique_schemes > 0:
                avg_records_per_scheme = len(nav_df) / unique_schemes
                print(f"Average records per scheme: {avg_records_per_scheme:.0f}")
            
            # Show sample data
            print(f"\nSample daily NAV data:")
            print(nav_df.head(10))
        
        return nav_df
    
    def safe_float_convert(self, value):
        """
        Safely convert value to float, handling various data types and edge cases
        """
        if pd.isna(value) or value is None:
            return None
        
        try:
            float_val = float(value)
            # Validate NAV range (reasonable bounds)
            if 0 < float_val < 100000:
                return float_val
            else:
                return None
        except (ValueError, TypeError):
            return None
    
    def create_hyper_file(self, metadata_df, nav_df, filename):
        """
        Create Tableau Hyper file with optimized batch inserts
        """
        try:
            # Create hyper_files folder in output directory
            project_root = os.path.dirname(self.script_dir)  # One level up to reach project root
            output_folder = os.path.join(project_root, "output", "hyper_files")
            os.makedirs(output_folder, exist_ok=True)
            
            # Create full filepath
            output_filepath = os.path.join(output_folder, filename)
            
            print(f"\nCreating Tableau Hyper file: {output_filepath}")
            
            with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                with Connection(endpoint=hyper.endpoint,
                              database=output_filepath,
                              create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
                    
                    # Define Fund_Metadata table schema
                    metadata_table = TableDefinition(
                        table_name=TableName("Fund_Metadata"),
                        columns=[
                            TableDefinition.Column("Scheme_Code", SqlType.text(), NOT_NULLABLE),
                            TableDefinition.Column("Scheme_Name", SqlType.text(), NULLABLE),
                            TableDefinition.Column("Fund_House", SqlType.text(), NULLABLE),
                            TableDefinition.Column("Scheme_Type", SqlType.text(), NULLABLE),
                            TableDefinition.Column("Scheme_Category", SqlType.text(), NULLABLE),
                            TableDefinition.Column("Scheme_Start_Date_Info", SqlType.text(), NULLABLE),
                            TableDefinition.Column("Current_NAV", SqlType.text(), NULLABLE),
                            TableDefinition.Column("Last_Updated", SqlType.text(), NULLABLE),
                            TableDefinition.Column("Main_Category", SqlType.text(), NULLABLE)
                        ]
                    )
                    
                    # Define Daily_NAV_Data table schema
                    nav_table = TableDefinition(
                        table_name=TableName("Daily_NAV_Data"),
                        columns=[
                            TableDefinition.Column("Scheme_Code", SqlType.text(), NOT_NULLABLE),
                            TableDefinition.Column("Date", SqlType.date(), NOT_NULLABLE),
                            TableDefinition.Column("NAV", SqlType.double(), NULLABLE),
                            TableDefinition.Column("Year", SqlType.int(), NULLABLE),
                            TableDefinition.Column("Month", SqlType.int(), NULLABLE),
                            TableDefinition.Column("Day", SqlType.int(), NULLABLE),
                            TableDefinition.Column("Weekday", SqlType.text(), NULLABLE)
                        ]
                    )
                    
                    # Create tables
                    connection.catalog.create_table(metadata_table)
                    connection.catalog.create_table(nav_table)
                    
                    print("Tables created in Hyper file")
                    
                    # Insert metadata with batch processing
                    if not metadata_df.empty:
                        print("Inserting metadata...")
                        with Inserter(connection, metadata_table) as inserter:
                            batch_size = 5000  # Larger batch for faster insertion
                            for start_idx in range(0, len(metadata_df), batch_size):
                                end_idx = min(start_idx + batch_size, len(metadata_df))
                                batch_df = metadata_df.iloc[start_idx:end_idx]
                                
                                for _, row in batch_df.iterrows():
                                    inserter.add_row([
                                        str(row.get('Scheme_Code', '')),
                                        str(row.get('Scheme_Name', '')) if pd.notna(row.get('Scheme_Name')) else None,
                                        str(row.get('Fund_House', '')) if pd.notna(row.get('Fund_House')) else None,
                                        str(row.get('Scheme_Type', '')) if pd.notna(row.get('Scheme_Type')) else None,
                                        str(row.get('Scheme_Category', '')) if pd.notna(row.get('Scheme_Category')) else None,
                                        str(row.get('Scheme_Start_Date_Info', '')) if pd.notna(row.get('Scheme_Start_Date_Info')) else None,
                                        str(row.get('Current_NAV')) if pd.notna(row.get('Current_NAV')) else None,
                                        str(row.get('Last_Updated', '')) if pd.notna(row.get('Last_Updated')) else None,
                                        str(row.get('Main_Category', '')) if pd.notna(row.get('Main_Category')) else None
                                    ])
                                
                                print(f"  Inserted metadata batch: {start_idx+1}-{end_idx}")
                            
                            inserter.execute()
                        print(f"Inserted {len(metadata_df)} metadata records")
                    
                    # Insert NAV data with batch processing
                    if not nav_df.empty:
                        with Inserter(connection, nav_table) as inserter:
                            batch_size = 50000  # Larger batch for NAV data
                            for start_idx in range(0, len(nav_df), batch_size):
                                end_idx = min(start_idx + batch_size, len(nav_df))
                                batch_df = nav_df.iloc[start_idx:end_idx]
                                
                                for _, row in batch_df.iterrows():
                                    try:
                                        date_obj = pd.to_datetime(row['Date']).date()
                                        
                                        inserter.add_row([
                                            str(row['Scheme_Code']),
                                            date_obj,
                                            self.safe_float_convert(row.get('NAV')),
                                            int(row['Year']) if pd.notna(row['Year']) else None,
                                            int(row['Month']) if pd.notna(row['Month']) else None,
                                            int(row['Day']) if pd.notna(row['Day']) else None,
                                            str(row['Weekday']) if pd.notna(row['Weekday']) else None
                                        ])
                                    except Exception as e:
                                        continue
                                
                                print(f"  Inserted NAV batch: {start_idx+1}-{end_idx}")
                            
                            inserter.execute()
                        print(f"Inserted {len(nav_df)} NAV data records")
            
            print(f"Tableau Hyper file '{output_filepath}' created successfully!")
            return True
            
        except HyperException as ex:
            print(f"Hyper Error: {ex}")
            return False
        except Exception as e:
            print(f"Error creating Hyper file: {str(e)}")
            return False
    
    def get_fund_summary_stats(self, metadata_df):
        """Display summary statistics"""
        print("\n" + "="*60)
        print("FUND SUMMARY STATISTICS")
        print("="*60)
        
        print(f"Total Mutual Funds: {len(metadata_df)}")
        
        if 'Main_Category' in metadata_df.columns:
            print("\nFunds by Category:")
            category_counts = metadata_df['Main_Category'].value_counts()
            for category, count in category_counts.items():
                print(f"  • {category}: {count}")
        
        if 'Fund_House' in metadata_df.columns:
            print(f"\nTotal Fund Houses: {metadata_df['Fund_House'].nunique()}")
            print("\nTop 10 Fund Houses by Number of Schemes:")
            top_fund_houses = metadata_df['Fund_House'].value_counts().head(10)
            for fund_house, count in top_fund_houses.items():
                print(f"  • {fund_house}: {count} schemes")
    
    def extract_fund_data(self, 
                         fund_house_filter=None,
                         category_filter=None, 
                         start_date=None,
                         end_date=None,
                         output_filename=None
                         ):
        """
        Main extraction function with optimized performance, caching, and flexible date ranges
        
        Args:
            fund_house_filter: Single ("Quant"), list (["Quant", "HDFC"]), or None (all)
            category_filter: Single ("Equity"), list (["Equity", "Debt"]), or None (all)
            start_date: Start date (datetime, 'YYYY-MM-DD', 'YYYY', or None for current YTD)
            end_date: End date (datetime, 'YYYY-MM-DD', 'YYYY', or None for today)
            output_filename: Custom filename or None for auto-generation
        """
        print("CACHED MUTUAL FUND DATA EXTRACTION - TABLEAU HYPER OUTPUT")
        print("=" * 70)
        print(f"Concurrency: {self.max_workers} threads | API delay: {self.api_delay}s")
        print(f"Cache directory: {self.cache_dir}")
        
        # Parse and display date range
        if start_date is None and end_date is None:
            parsed_start, parsed_end = self._get_default_date_range()
            date_info = f"Current_YTD_{parsed_start.year}"
        else:
            parsed_start = self._parse_date_input(start_date) if start_date else self._get_default_date_range()[0]
            parsed_end = self._parse_date_input(end_date) if end_date else datetime.now()
            parsed_start, parsed_end = self._validate_date_range(parsed_start, parsed_end)
            
            if parsed_start.year == parsed_end.year:
                date_info = f"{parsed_start.year}"
                if parsed_start.month != 1 or parsed_start.day != 1 or parsed_end != datetime(parsed_end.year, 12, 31):
                    date_info = f"{parsed_start.strftime('%Y%m%d')}_{parsed_end.strftime('%Y%m%d')}"
            else:
                date_info = f"{parsed_start.strftime('%Y%m%d')}_{parsed_end.strftime('%Y%m%d')}"
        
        print(f"Date range: {parsed_start.strftime('%Y-%m-%d')} to {parsed_end.strftime('%Y-%m-%d')}")
     

        # Auto-generate filename if not provided
        if output_filename is None:
            parts = []
            if fund_house_filter:
                if isinstance(fund_house_filter, list):
                    parts.append("_".join(fund_house_filter))
                else:
                    parts.append(str(fund_house_filter))
            else:
                parts.append("AllFunds")
            
            if category_filter:
                if isinstance(category_filter, list):
                    parts.append("_".join(category_filter))
                else:
                    parts.append(str(category_filter))
            
            parts.append(f"Daily_{date_info}")
            output_filename = f"{'_'.join(parts)}.hyper"
        else:
            if not output_filename.endswith('.hyper'):
                output_filename += '.hyper'
        
        # Create full output path
        output_folder = os.path.join(self.script_dir, "hyper_files")
        output_filepath = os.path.join(output_folder, output_filename)
        print(f"Output file: {output_filepath}")
        
        # Step 1: Get metadata with filtering
        metadata_df = self.get_mutual_fund_metadata(fund_house_filter, category_filter)
        if metadata_df.empty:
            print("No funds found matching the criteria. Exiting...")
            return
        
        # Show summary statistics
        self.get_fund_summary_stats(metadata_df)
        
        # Step 2: Get scheme codes for NAV data
        scheme_codes = metadata_df['Scheme_Code'].tolist()
        
        # Step 3: Fetch daily NAV data with caching and date range
        nav_df = self.fetch_daily_nav_data(scheme_codes, start_date, end_date)
        if nav_df.empty:
            print("Warning: No NAV data fetched. Creating Hyper file with metadata only...")
            nav_df = pd.DataFrame(columns=['Scheme_Code', 'Date', 'NAV', 'Year', 'Month', 'Day', 'Weekday'])
        
        # Step 4: Create Hyper file
        success = self.create_hyper_file(metadata_df, nav_df, output_filename)
        
        # Save both caches after successful extraction
        self._save_nav_cache()
        self._save_metadata_cache()
        
        # Clean up log files
        self._cleanup_log_files()
        
        if success:
            print("\n" + "="*70)
            print("CACHED EXTRACTION COMPLETED SUCCESSFULLY!")
            print("="*70)
            print(f"File saved as: {output_filepath}")
            print(f"Metadata records: {len(metadata_df)}")
            print(f"Daily NAV data records: {len(nav_df)}")
            
            # Show cache performance summary
            total_requests = self._get_cache_stat('hits') + self._get_cache_stat('misses')
            if total_requests > 0:
                hit_rate = (self._get_cache_stat('hits') / total_requests) * 100
                api_calls_saved = self._get_cache_stat('hits')
                print(f"\nCACHE PERFORMANCE:")
                print(f"  • Cache hit rate: {hit_rate:.1f}%")
                print(f"  • API calls saved: {api_calls_saved}")
                print(f"  • New data cached: {self._get_cache_stat('new_data')} records")
            
            print(f"\nHyper file contains:")
            print(f"  • Table 1 (Fund_Metadata): Fund details with filters applied")
            print(f"  • Table 2 (Daily_NAV_Data): Daily NAV data from {parsed_start.strftime('%Y-%m-%d')} to {parsed_end.strftime('%Y-%m-%d')}")
            
            if not nav_df.empty:
                print(f"\nDaily NAV Summary:")
                print(f"  • Date range: {nav_df['Date'].min()} to {nav_df['Date'].max()}")
                print(f"  • Total daily records: {len(nav_df)}")
                if 'Scheme_Code' in nav_df.columns:
                    unique_schemes = nav_df['Scheme_Code'].nunique()
                    print(f"  • Schemes with data: {unique_schemes}")
                    if unique_schemes > 0:
                        avg_days_per_scheme = len(nav_df) / unique_schemes
                        print(f"  • Average days per scheme: {avg_days_per_scheme:.0f}")
            
            
        else:
            print("Process failed. Please check error messages above.")
    

# Usage Examples and Main Execution
if __name__ == "__main__":
    # Record start time for total execution tracking
    total_start_time = time.time()
    
    print("="*70)
    print("OPTIMIZED MUTUAL FUND DATA EXTRACTOR - TABLEAU HYPER")
    print("Using mftool library with concurrent processing")
    print("="*70)
    
    
    # Create extractor instance with OPTIMIZED settings to avoid API rate limiting
    # Reduced workers and increased delays to prevent timeouts
    extractor = OptimizedMutualFundExtractor(
        max_workers=15,    # Reduced from 100 to avoid rate limiting
        api_delay=0.5      # Increased from 0.1s to respect API limits
    )
    
    
    # Show current cache status and benefits
    extractor.show_cache_growth_benefits()
    
    extractor.extract_fund_data(
     start_date="2010-01-01"
   )
    
    # Uncomment below for historical data (run after cache is populated):
    # extractor.extract_fund_data(
    #     start_date="2020-01-01",  # Last 4 years
    #     end_date=f"{current_year}-12-31"
    # )

    
    print("\n" + "="*70)
    print("DATE RANGE INPUT OPTIONS:")
    print("="*70)
    print("  None (default): Current YTD (Jan 1 to today)")
    print("  String format: '2024' or '2024-06-15'")
    print("  Datetime objects: datetime(2024, 6, 15)")
    print("="*70)

    # Show updated cache statistics after extraction
    extractor.get_cache_stats()
    
    # Show cache health and recommendations
    extractor.check_cache_health()
    
    # Calculate and display total execution time
    total_end_time = time.time()
    total_execution_time = total_end_time - total_start_time
    
    print("\n" + "="*70)
    print("TOTAL EXECUTION TIME SUMMARY")
    print("="*70)            
        
    print(f"Started at: {datetime.fromtimestamp(total_start_time).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Ended at: {datetime.fromtimestamp(total_end_time).strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    if total_execution_time < 60:
        print(f"Total execution time: {total_execution_time:.2f} seconds")
    elif total_execution_time < 3600:
        minutes = int(total_execution_time // 60)
        minutes_remainder = total_execution_time % 60
        print(f"Total execution time: {minutes} minutes {minutes_remainder:.2f} seconds")
    else:
        hours = int(total_execution_time // 3600)
        minutes = int((total_execution_time % 3600) // 60)
        seconds = total_execution_time % 60
        print(f"Total execution time: {hours} hours {minutes} minutes {seconds:.2f} seconds") 

