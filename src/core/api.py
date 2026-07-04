"""
Creator: @Saket
Email: anshusaketsingh@gmail.com
"""

import threading
import time
import socket
from mftool import Mftool

# Set a global timeout so that API requests don't hang indefinitely
socket.setdefaulttimeout(15.0)

class MftoolWrapper:
    """Wrapper around mftool to provide thread-safe, rate-limited API access."""
    def __init__(self, api_delay: float = 0.05):
        self.mf = Mftool()
        self.api_delay = api_delay
        self.api_lock = threading.Lock()

    def _rate_limited_api_call(self, func, *args, **kwargs):
        max_retries = 3
        for attempt in range(max_retries):
            with self.api_lock:
                # Increase delay slightly on retries (backoff)
                time.sleep(self.api_delay * (attempt + 1))
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"Failed {func.__name__} after {max_retries} attempts.")
                    raise
                print(f"API Error in {func.__name__} (Attempt {attempt + 1}/{max_retries}): {e}. Retrying...")

    def get_scheme_codes(self) -> dict:
        """Fetch all scheme codes mapping to their names."""
        return self._rate_limited_api_call(self.mf.get_scheme_codes)

    def get_scheme_details(self, code: str) -> dict:
        """Fetch detailed metadata for a specific scheme code."""
        return self._rate_limited_api_call(self.mf.get_scheme_details, code)

    def get_scheme_historical_nav(self, code: str, as_Dataframe: bool = True):
        """Fetch the entire historical NAV series for a scheme."""
        return self._rate_limited_api_call(self.mf.get_scheme_historical_nav, code, as_Dataframe=as_Dataframe)
