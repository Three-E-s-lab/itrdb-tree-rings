"""
ITRDB Canada NOAA File Fetcher

Fetches only *-rwl-noaa.txt files from ITRDB Canada database.
These files follow the NOAA template and contain rich metadata as well as raw ring-width measurements.

Features:
- Downloads only NOAA format files (*-rwl-noaa.txt)
- Handles elevation -999 as NaN
- Detects measurement units from end-of-series markers
- Creates comprehensive manifest

Usage:
# Download all NOAA files from measurements
    python fetch_itrdb_ca_noaa.py --max-workers 16 --resume
    
# Scan only (create manifest without downloading noaa-rwl.txt files)
    python fetch_itrdb_ca_noaa.py --scan-only

# Download from both measurements and chronologies
    python fetch_itrdb_ca_noaa.py --roots both --max-workers 16
"""

from __future__ import annotations
import argparse, csv, re, sys, time, hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import threading
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# tqdm for progress bars
try:
    # Try to import tqdm (a popular progress bar library)
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    # If tqdm isn't installed, set flag to False and define a fallback class
    HAS_TQDM = False
    
    # Define a minimal custom tqdm class to mimic the real one
    class tqdm:
        def __init__(self, iterable=None, total=None, desc=None, unit='it', **kwargs):
            # Store the iterable or default to an empty list
            self.iterable = iterable or []
            # If 'total' is not given, try to infer it from the iterable's length
            self.total = total or len(self.iterable) if hasattr(self.iterable, '__len__') else 0
            # Description to show before the progress count
            self.desc = desc or ""
            # Counter for how many items have been processed
            self.n = 0
        def __iter__(self):
            # Yield items one by one while updating the progress
            for item in self.iterable:
                yield item
                self.update(1)
        def update(self, n=1):
            # Increment the counter
            self.n += n
            # Print progress every 50 steps or when reaching the end
            if self.n % 50 == 0 or self.n >= self.total:
                print(f"\r{self.desc}: {self.n}/{self.total}", end="", flush=True)
        def __enter__(self): 
            # Support "with tqdm(...)" context manager usage
            return self
        def __exit__(self, *args): 
            # Print a newline when the context block ends
            print()

# Base URL for NOAA's (National Centers for Environmental Information) data repository
BASE = "https://www.ncei.noaa.gov"

# Paths to relevant subdirectories for Canadian tree ring data
ROOTS = {
    "chronologies": "/pub/data/paleo/treering/chronologies/northamerica/canada/",
    "measurements": "/pub/data/paleo/treering/measurements/northamerica/canada/",
}

# Pattern to match NOAA ring-width measurement files
# Examples:
#   mysite-rwl-noaa.txt
#   mysite-rwl-noaa 2.txt  (copy number variants)
NOAA_FILE_PATTERN = re.compile(r'^.*-rwl-noaa(?:\s+\d+)?\.txt$', re.IGNORECASE) # regex matches filenames ending with -rwl-noaa.txt or -rwl-noaa N.txt and make matching case-insensitive

# Helper function to build regex for extracting metadata fields from NOAA text files
# Example: "# Title: Some Site Info"
def make_field_regex(label: str) -> re.Pattern:
    return re.compile(rf"(?im)^\s*#\s*{re.escape(label)}\s*:\s*(.+?)\s*$")
    # (?im) -> case-insensitive + multiline mode
    # ^\s*#   -> line starts with optional spaces and '#'
    # label   -> the field name we're looking for (escaped to avoid regex meta issues)
    # : value -> captures everything after the colon as the field's value

# Dictionary of regex patterns for NOAA metadata fields.
# Each entry maps a descriptive key (our internal name) to a regex
# that extracts the corresponding field from a NOAA tree-ring file.    
NOAA_PATTERNS = {
    # Site identifiers
    "site_name": make_field_regex("Site_Name"),
    "study_name": make_field_regex("Study_Name"),
    "location": make_field_regex("Location"),
    "collection_name": make_field_regex("Collection_Name"),
    
    # Species information
    "species_name": make_field_regex("Species_Name"),
    "common_name": make_field_regex("Common_Name"),
    "species_code": make_field_regex("Tree_Species_Code"),
    
    # Temporal coverage
    "earliest_year": make_field_regex("Earliest_Year"),
    "latest_year": make_field_regex("Most_Recent_Year"),
    "first_year": make_field_regex("First_Year"),
    "last_year": make_field_regex("Last_Year"),
    
    # Elevation (can appear in different formats)
    "elevation": make_field_regex("Elevation"),
    "elevation_m": make_field_regex("Elevation_m"),
    
    # Geographic coordinates (bounding box)
    "nlat": make_field_regex("Northernmost_Latitude"),
    "slat": make_field_regex("Southernmost_Latitude"),
    "elon": make_field_regex("Easternmost_Longitude"),
    "wlon": make_field_regex("Westernmost_Longitude"),
}

# Abive ensures all regex patterns are compiled, e.g.,
# Site_Name: Banff National Park
# Species_Name: Picea glauca
# Earliest_Year: 1420

# A simple data container describing a single file or directory entry (good for crawling or indexing remote data).
@dataclass
class FileEntry:
    root_key: str   # Key identifying which root (chronologies / measurements) this file belongs to
    url: str        # Full URL of the file on NOAA server
    relpath: str    # Relative path from the root directory (for organizing downloads)
    name: str       # File or directory name
    is_dir: bool    # True if this entry is a directory
    size: int = -1  # Size in bytes (if known), -1 if unknown

# Tracks file downloads: how many succeeded/failed, total size, elapsed time, estimated time remaining, and throughput - all updated safely in multi-threaded contexts.
@dataclass
class ProgressTracker:
    total: int = 0              # Total number of files expected
    completed: int = 0          # Number of successful downloads
    failed: int = 0             # Number of failed downloads
    start_time: float = field(default_factory=time.time)  # Time when tracking started
    bytes_downloaded: int = 0   # Total number of bytes downloaded so far
    # If multiple threads all try to update the same variable (e.g., completed count), they might overwrite each other’s changes.
    # The lock ensures only one thread at a time updates the ProgressTracker, keeping counts accurate.
    lock: threading.Lock = field(default_factory=threading.Lock)  # Lock to synchronize updates in multi-threaded code 
    
    def update(self, success: bool = True, bytes_count: int = 0):
        """
        Update progress counts. Thread-safe.
        - success=True increments completed count, else increments failed count.
        - bytes_count adds to total bytes downloaded.
        """
        with self.lock:
            if success:
                self.completed += 1
            else:
                self.failed += 1
            self.bytes_downloaded += bytes_count
    
    @property
    def elapsed_time(self) -> float:
        """Return the total elapsed time (seconds) since tracking started."""
        return time.time() - self.start_time
    
    @property
    def eta(self) -> Optional[float]:
        """
        Estimate remaining time (seconds) until completion.
        Based on average file completion rate.
        Returns None if not enough data to estimate.
        """
        if self.completed == 0:
            return None
        rate = self.completed / self.elapsed_time
        remaining = self.total - (self.completed + self.failed)
        return remaining / rate if rate > 0 else None
    
    @property
    def throughput_mb_s(self) -> float:
        """
        Calculate average download throughput (MB/s).
        Returns 0 if no time has passed yet.
        """
        if self.elapsed_time == 0:
            return 0
        return (self.bytes_downloaded / (1024 * 1024)) / self.elapsed_time

class OptimizedSession:
    def __init__(self, max_retries: int = 5, backoff_factor: float = 1.0):
        # Create a single persistent HTTP session (reuses TCP connections = faster)
        self.session = requests.Session()
        
        # Retry policy for transient errors (rate limits, timeouts, server hiccups)
        retry_kwargs = {
            'total': max_retries,                           # max total retry attempts
            'status_forcelist': [429, 500, 502, 503, 504],  # retry only on these HTTP codes
            'backoff_factor': backoff_factor,               # wait time between retries (exponential backoff): {backoff factor} * (2 ** (retry count - 1))
            'raise_on_status': False                        # don't raise on certain HTTP statuses 
        }
        
        try:
            # Newer urllib3 uses 'allowed_methods'
            retry_kwargs['allowed_methods'] = ["HEAD", "GET", "OPTIONS"]
            retry_strategy = Retry(**retry_kwargs)
        except TypeError:
            # Older urllib3 uses 'method_whitelist'
            retry_kwargs['method_whitelist'] = ["HEAD", "GET", "OPTIONS"]
            retry_strategy = Retry(**retry_kwargs)
        
        # HTTPAdapter configures connection pooling + retries for the session
        adapter = HTTPAdapter(
            max_retries=retry_strategy, # attach the retry policy
            pool_connections=32,        # max keep-alive connections per host in the pool
            pool_maxsize=32,            # max concurrent connections in the pool
            pool_block=True             # block if pool is exhausted (instead of raising error)
        )
        
        # Mount the adapter so both HTTP and HTTPS requests use this pooling/retry behavior
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
        # Set sane default headers (UA helps avoid being blocked by some servers -- makes sure we look like a real browser and not a bot)
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; ITRDB-Fetcher/5.0; Scientific Research)",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        })
    
    def get(self, url: str, **kwargs) -> requests.Response:
        # Default timeouts: (connect_timeout, read_timeout)
        kwargs.setdefault('timeout', (15, 60))
        return self.session.get(url, **kwargs)
    
    def head(self, url: str, **kwargs) -> requests.Response:
        # HEAD requests usually return quickly; use shorter defaults
        kwargs.setdefault('timeout', (10, 30))
        return self.session.head(url, **kwargs)

def to_float_safe(s: Optional[str]) -> Optional[float]:
    """
    Turn a string into a float safely.
    - If it's empty or None -> return None
    - If it's "-999" (a NOAA "missing value" code) -> treat as None
    - If it can't be converted -> return None
    """
    if s is None or str(s).strip() == "":
        return None
    try:
        val = float(str(s).strip())
        return None if val == -999 else val  # Handle -999 as NaN
    except Exception:
        return None

def group1(regex: re.Pattern, text: str) -> Optional[str]:
    """
    Apply a regex to the text.
    If it finds something, return the *first capture group* (the bit in parentheses).
    If not found -> return None.
    """
    m = regex.search(text)
    return m.group(1).strip() if m else None

def detect_units(text: str) -> str:
    """
    Figure out the measurement units from special "end markers":
    - If file ends with -9999 -> data is in 0.001 mm
    - If file ends with 999   -> data is in 0.01 mm
    Otherwise -> "unknown"
    """
    # Look for data section and check for end markers
    if re.search(r'\b-9999\b', text):
        return "0.001 mm"
    elif re.search(r'\b999\b', text):
        return "0.01 mm"
    return "unknown"

def parse_noaa_file(text: str, filename: str) -> Dict[str, Optional[str]]:
    """Read NOAA tree-ring file text and pull out useful metadata."""
    
    # Extract coordinates (lat/lon). Sometimes NOAA gives "north" and "south", so we take the midpoint if both exist.
    nlat = to_float_safe(group1(NOAA_PATTERNS["nlat"], text))
    slat = to_float_safe(group1(NOAA_PATTERNS["slat"], text))
    elon = to_float_safe(group1(NOAA_PATTERNS["elon"], text))
    wlon = to_float_safe(group1(NOAA_PATTERNS["wlon"], text))
    
    lat = lon = None
    if nlat is not None and slat is not None:
        lat = (nlat + slat) / 2.0
    elif nlat is not None:
        lat = nlat
    elif slat is not None:
        lat = slat
    
    if elon is not None and wlon is not None:
        lon = (elon + wlon) / 2.0
    elif elon is not None:
        lon = elon
    elif wlon is not None:
        lon = wlon
    
    # Site name (prefer Site_Name, fallback to Study_Name)
    site_name = group1(NOAA_PATTERNS["site_name"], text) or group1(NOAA_PATTERNS["study_name"], text)
    
    # Elevation (handle -999 as NaN)
    elevation = to_float_safe(group1(NOAA_PATTERNS["elevation"], text) or group1(NOAA_PATTERNS["elevation_m"], text))
    
    # Years
    year_start = group1(NOAA_PATTERNS["earliest_year"], text) or group1(NOAA_PATTERNS["first_year"], text)
    year_end = group1(NOAA_PATTERNS["latest_year"], text) or group1(NOAA_PATTERNS["last_year"], text)
    
    # Collection ID from filename (e.g., cana096 from cana096-rwl-noaa.txt)
    collection_id = ""
    stem_match = re.search(r'\b(can[a-z0-9]+)\b', filename, re.IGNORECASE)
    if stem_match:
        collection_id = stem_match.group(1).upper()
    
    # Detect units
    units = detect_units(text)
    
    return {
        "site_name": site_name or "",
        "location": group1(NOAA_PATTERNS["location"], text) or "",
        "species_name": group1(NOAA_PATTERNS["species_name"], text) or "",
        "common_name": group1(NOAA_PATTERNS["common_name"], text) or "",
        "species_code": group1(NOAA_PATTERNS["species_code"], text) or "",
        "elevation_m": f"{elevation:.1f}" if elevation is not None else "",
        "year_start": year_start or "",
        "year_end": year_end or "",
        "collection_id": group1(NOAA_PATTERNS["collection_name"], text) or collection_id,
        "lat": f"{lat:.6f}" if lat is not None else "",
        "lon": f"{lon:.6f}" if lon is not None else "",
        "units": units,
    }

def crawl_directory(session: OptimizedSession, root_key: str, max_depth: int = 5) -> List[FileEntry]:
    """Crawl directory structure and find all NOAA files."""
    
    # Start from the base URL for the chosen root (e.g., measurements folder).
    base_url = f"{BASE}{ROOTS[root_key]}"
    entries = [] # keep a list of entires
    visited = set() # Keep track of visited URLs to avoid cycles/duplication
    
    def crawl_recursive(url: str, depth: int) -> List[FileEntry]:
        # This is the engine. It visits one page, grabs links, and if it finds subfolders, goes inside them (recursion).
        # If we’ve gone too deep or already visited this URL -> stop.
        # Otherwise, mark this URL as visited.
        if depth > max_depth or url in visited:
            return []
        
        visited.add(url)
        try:
            # Download the page.
            # If it fails (404, etc.), it will raise an error and skip.
            response = session.get(url)
            response.raise_for_status()

            # Find all href="..." links on the page. These are either:
                # directories (something/)
                # files (something.txt)
                # or junk (../, #, query params).            
            links = re.findall(r'href="([^"]+)"', response.text, re.IGNORECASE)
            local_entries = []
            
            for link in links:
                if link.startswith(('?', '#', '../', './')): # skip junk links
                    continue
                
                full_url = urljoin(url, link)   # make full URL
                name = link.rstrip('/').split('/')[-1]  # get the name (last part of path) just the file/folder name
                is_dir = link.endswith('/') # check if its a folder (based on trailing slash)
                
                if full_url.startswith(base_url): # only crawl within the base URL
                    relpath = full_url.replace(base_url, '').lstrip('/')    # relative path from root
                    
                    # Only include NOAA files or directories
                    # If it's a directory, we want to crawl inside it so create a FileEntry for it, add it.
                    # thne go inside recursively to look for files in that folder
                    if is_dir:
                        entry = FileEntry(root_key=root_key, url=full_url, relpath=relpath, name=name, is_dir=True)
                        local_entries.append(entry)
                        local_entries.extend(crawl_recursive(full_url, depth + 1))
                    elif NOAA_FILE_PATTERN.match(name): # if it's a file and matches NOAA pattern -> save it!
                        entry = FileEntry(root_key=root_key, url=full_url, relpath=relpath, name=name, is_dir=False) 
                        local_entries.append(entry)
            return local_entries
        
        # If anything goes wrong downloading/parsing the page, log a warning and skip.    
        except Exception as e:
            print(f"[WARNING] Failed to crawl {url}: {e}")
            return []
    
    # Print a message so you know it’s working.
    # Call the recursive function starting from the base URL, depth = 0.
    print(f"[INFO] Crawling {root_key} for NOAA files (max depth: {max_depth})...")
    entries = crawl_recursive(base_url if base_url.endswith('/') else base_url + '/', 0)
    
    return [e for e in entries if not e.is_dir]  # Return only files

def analyze_noaa_file(session: OptimizedSession, entry: FileEntry) -> Optional[Dict]:
    """Analyze a single NOAA file and extract metadata."""
    try:
        # Get file size fromt the server (HEAD request)
        file_size = -1 # default if unknown
        try:
            head_response = session.head(entry.url) # HEAD request to get headers only
            if head_response.ok and 'content-length' in head_response.headers:
                file_size = int(head_response.headers['content-length'])
        except Exception:
            # If the HEAD request fails, just ignore and leave file_size = -1
            pass
        
        # Download file content (GET request)
        try:
            response = session.get(entry.url)   # fetch the actual file
            response.raise_for_status()         # raise error if not 200 OK
            content = response.text             # file content as text
        except Exception as e:
            print(f"[ERROR] Failed to download {entry.name}: {e}")
            return None
        
        # Parse metadata from the file text
        metadata = parse_noaa_file(content, entry.name)
        # This extracts fields like site name, species, years, coords, etc.
        # Build result dictionary with all info
        result = {
            'root': entry.root_key,         # measurements / chronologies
            'filename': entry.name,         # just the file name
            'path': entry.relpath,          # relative path in NOAA directory structure
            'url': entry.url,               # full URL to download
            'file_size_bytes': file_size,   # size in bytes (or -1 if unknown)
            
            # Metadata extracted from the NOAA file
            'site_name': metadata['site_name'],
            'location': metadata['location'],
            'collection_id': metadata['collection_id'],
            'species_code': metadata['species_code'],
            'species_name': metadata['species_name'],
            'common_name': metadata['common_name'],
            'lat': metadata['lat'],
            'lon': metadata['lon'],
            'elevation_m': metadata['elevation_m'],
            'year_start': metadata['year_start'],
            'year_end': metadata['year_end'],
            'units': metadata['units'],
            
            # A short checksum from the URL (for quick uniqueness checking) -> acts as a simple ID.
            'checksum': hashlib.md5(entry.url.encode()).hexdigest()[:8],
            
            # Status of download/processing (starts as "pending")
            'download_status': 'pending',
        }
        
        return result
        
    except Exception as e:
        # If something unexpected goes wrong, log it and return None
        print(f"[ERROR] Analysis failed for {entry.name}: {e}")
        return None

def download_file(session: OptimizedSession, entry: FileEntry, output_path: Path, resume: bool = True) -> Tuple[bool, int]:
    """
    Downloads a file from NOAA.
    Supports resume, meaning if you stopped halfway, it can continue instead of starting over.
    Returns a tuple:
        bool -> success or failure
        int -> number of bytes downloaded
    """
    
    # Make sure the folder exists for saving the file.
    # Save into a temporary file (with .part at the end) while downloading.
    # When done, rename .part -> final file. (Avoids leaving half-finished files with the real name.)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_suffix(output_path.suffix + '.part')
    
    # Check for resume
    # If resume is on and there’s already a .part file:
        # figure out how many bytes we already downloaded (start_byte).
        # If not, we start from 0.
    start_byte = 0
    if resume and temp_path.exists():
        start_byte = temp_path.stat().st_size
    
    # If resuming, tell the server:
    # "Please send me the file starting from byte X onward" A.K.A. HTTP Range request
    headers = {}
    if start_byte > 0:
        headers['Range'] = f'bytes={start_byte}-'
    
    try:
        # ask for the file, if status is 200 (OK) or 206 (Partial Content) -> good, otherwise fail.
        with session.get(entry.url, headers=headers, stream=True) as response: # stream=True means we get data in chunks, not all at once
            if response.status_code not in (200, 206):
                return False, 0
            # if resuming -> open file in append mode (ab)
            # if new download -> open in write mode (wb)
            mode = 'ab' if start_byte > 0 else 'wb'
            bytes_written = start_byte
            # Read file in chunks (64 KB at a time) and write to disk
            # keep track of how many bytes we wrote
            with open(temp_path, mode) as f:
                for chunk in response.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)
                        bytes_written += len(chunk)
            
            # when finished, rename .part -> final filename
            temp_path.rename(output_path)
            # return success and bytes written
            return True, bytes_written
    
    # if anything goes wrong, print an error and return failure.        
    except Exception as e:
        print(f"[ERROR] Failed to download {entry.name}: {e}")
        return False, 0

def write_manifest(output_path: Path, results: List[Dict]) -> None:
    """"Write manifest CSV (metadata of all NOAA files we processed)."""
    
    # Make sure the folder exists where the CSV will be saved
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # These are the columns we want in the CSV
    fieldnames = [
        'root', 'filename', 'path', 'url', 'file_size_bytes',
        'site_name', 'location', 'collection_id',
        'species_code', 'species_name', 'common_name',
        'lat', 'lon', 'elevation_m', 'year_start', 'year_end',
        'units', 'checksum', 'download_status'
    ]
    
    # Open the output file for writing (UTF-8 so it handles special characters)
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)   # CSV writer knows column order
        writer.writeheader()                                # Write the header row with all fieldnames
        
        # Go through each result dictionary and write it as one row in the CSV
        for result in results:
            # Filter the dict: keep only the keys we defined in fieldnames
            # If a key is missing, fill with an empty string
            filtered_result = {k: result.get(k, '') for k in fieldnames}
            
            # Write that dictionary as a row
            writer.writerow(filtered_result)

def main():
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Simplified ITRDB Canada NOAA File Fetcher", # brief description
        epilog="Example: python fetch_itrdb_ca_noaa.py --max-workers 16 --resume", # example usage
        formatter_class=argparse.RawDescriptionHelpFormatter    # keep formatting as-is
    )
    
    parser.add_argument('--output', default='./itrdb_canada_noaa',
                    help='Output directory')
    parser.add_argument('--roots', default='measurements',
                    choices=['measurements', 'chronologies', 'both'],
                    help='Data types to fetch')
    parser.add_argument('--max-workers', type=int, default=16,
                    help='Maximum concurrent workers')
    parser.add_argument('--resume', action='store_true',
                    help='Resume partial downloads')
    parser.add_argument('--scan-only', action='store_true',
                    help='Only scan and create manifest (no downloads)')
    parser.add_argument('--manifest', default='itrdb_canada_noaa_manifest.csv',
                    help='Manifest filename')
    parser.add_argument('--max-depth', type=int, default=5,
                    help='Maximum crawl depth')
    
    args = parser.parse_args()
    
    # Determine which NOAA roots to process
    if args.roots == 'both':
        roots_to_process = ['measurements', 'chronologies']
    else:
        roots_to_process = [args.roots]
    
    print(f"[INFO] Fetching NOAA files (*-rwl-noaa.txt)")
    print(f"[INFO] Target roots: {', '.join(roots_to_process)}")
    print(f"[INFO] Output directory: {Path(args.output).resolve()}")
    
    # Create an HTTP session with retries, pooling, and timeouts
    session = OptimizedSession()
    
    # Phase 1: Crawling directories to find all NOAA files
    all_entries = []
    for root_key in roots_to_process: # for each root (measurements/chronologies)
        entries = crawl_directory(session, root_key, args.max_depth) # crawl and find all NOAA files
        all_entries.extend(entries) # add to the master list
        print(f"[INFO] Found {len(entries)} NOAA files in {root_key}") # report how many found in this root
    
    print(f"\n[INFO] Total NOAA files found: {len(all_entries)}")   # total across all roots
    
    if not all_entries: # if none found, exit early
        print("[WARNING] No NOAA files found!")
        return 0
    
    # Phase 2: Analysis (read + parse metadata)
    print(f"\n[PHASE 2] Analyzing {len(all_entries)} NOAA files...")
    
    # Analyze files in parallel using threads
    results = [] # will hold per-file metadata dicts
    progress = ProgressTracker(total=len(all_entries)) # track analysis progress
    
    # Run analyses in parallel for speed
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        # Map each future to its corresponding FileEntry
        future_to_entry = {
            executor.submit(analyze_noaa_file, session, entry): entry # submit analysis task
            for entry in all_entries # for each file entry
        }

        # Use tqdm to show progress bar if available
        with tqdm(total=len(all_entries), desc="Analyzing", unit="files", disable=not HAS_TQDM) as pbar:
            for future in as_completed(future_to_entry): # as each analysis completes
                result = future.result() # get the result dict (or None if failed)
                if result:
                    results.append(result) # add to results if successful
                progress.update(success=result is not None) # update progress tracker
                pbar.update(1) # update progress bar
    
    print(f"\n[INFO] Successfully analyzed {len(results)} files")
    
    # Quick statistics (units distribution and most common species codes)
    units_stats = defaultdict(int) # count of files per unit type
    species_stats = defaultdict(int) # count of files per species code
    
    # Gather stats
    for r in results:
        units_stats[r.get('units', 'unknown')] += 1 # count units
        if r.get('species_code'):
            species_stats[r['species_code']] += 1 # count species codes
    
    print(f"\n[UNITS DISTRIBUTION]:")
    for unit, count in sorted(units_stats.items()): # sort by unit name
        print(f"  {unit}: {count}") # print unit and count
    
    if species_stats: # only if we have species codes
        print(f"\n[TOP SPECIES CODES]:") # show top 10 species codes
        for species, count in sorted(species_stats.items(), key=lambda x: x[1], reverse=True)[:10]: # sort by count desc, take top 10
            print(f"  {species}: {count}") # print species code and count
    
    # Sort results
    results.sort(key=lambda x: (x.get('root', ''), x.get('filename', '')))
    
    # Write manifest
    output_dir = Path(args.output) # output directory path
    output_dir.mkdir(parents=True, exist_ok=True) # ensure it exists
    manifest_path = output_dir / args.manifest # full path to manifest file
    
    write_manifest(manifest_path, results) # write the CSV manifest
    print(f"\n[INFO] Manifest written: {manifest_path}") # report manifest location
    
    # if scan-only mode, exit here
    if args.scan_only: 
        print(f"\n[DONE] Scan-only mode completed.")
        return 0
    
    # Phase 3: Downloads
    print(f"\n[PHASE 3] Downloading {len(results)} files...")
    
    download_progress = ProgressTracker(total=len(results)) # track download progress
    download_stats = {'success': 0, 'failed': 0, 'skipped': 0} # counts of download outcomes
    
    # Download function for a single file (used in threads)
    # One task = download one file (built from the manifest row)
    def download_task(result_dict: Dict) -> Dict:
        entry = FileEntry( 
            root_key=result_dict['root'],
            url=result_dict['url'],
            relpath=result_dict['path'],
            name=result_dict['filename'],
            is_dir=False,
            size=result_dict['file_size_bytes']
        )
        
        # Save files in a flat structure (all into output_dir)
        output_path = output_dir / entry.name
        
        # Skip if already exists and resume is OFF
        if output_path.exists() and not args.resume:
            result_dict['download_status'] = 'exists'
            download_progress.update(success=True)
            return result_dict
        
        # Try the download (with resume support if enabled)
        success, bytes_downloaded = download_file(session, entry, output_path, args.resume)
        
        result_dict['download_status'] = 'success' if success else 'failed'
        download_progress.update(success=success, bytes_count=bytes_downloaded)
        
        return result_dict
    
    updated_results = []
    
    # Download in parallel for speed
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        # Map each future to its corresponding result dict
        future_to_result = {executor.submit(download_task, result): result for result in results}
        
        # Use tqdm to show progress bar if available
        with tqdm(total=len(results), desc="Downloading", unit="files", disable=not HAS_TQDM) as pbar:
            for future in as_completed(future_to_result):
                result = future.result()
                updated_results.append(result)
                
                # Update counters for pretty printing
                status = result.get('download_status', '')
                if status == 'success':
                    download_stats['success'] += 1
                elif status == 'failed':
                    download_stats['failed'] += 1
                else:
                    download_stats['skipped'] += 1
                
                pbar.update(1)
                
                # Show live throughput and ETA if tqdm is available
                if HAS_TQDM and download_progress.completed > 0:
                    eta_str = f"{download_progress.eta:.0f}s" if download_progress.eta else "∞"
                    throughput = download_progress.throughput_mb_s
                    pbar.set_postfix({
                        'success': download_stats['success'],
                        'failed': download_stats['failed'],
                        'MB/s': f"{throughput:.1f}",
                        'ETA': eta_str
                    })
    
    # Update manifest with final download_status for each file
    write_manifest(manifest_path, updated_results)
    
    # Summary - Easy-to-read stats
    total_mb = download_progress.bytes_downloaded / (1024 * 1024)  # total MB downloaded (convert bytes to MB)
    elapsed_mins = download_progress.elapsed_time / 60
    
    print(f"\n[COMPLETED] Download Summary:")
    print(f"  Total files: {len(results)}")
    print(f"  Successfully downloaded: {download_stats['success']}")
    print(f"  Failed: {download_stats['failed']}")
    print(f"  Skipped (existing): {download_stats['skipped']}")
    print(f"  Total data: {total_mb:.1f} MB")
    print(f"  Total time: {elapsed_mins:.1f} minutes")
    print(f"  Final manifest: {manifest_path}")
    print(f"\n[SUCCESS] All files saved to: {output_dir}")
    
    # Return a nonzero exit code if any downloads failed (useful in scripts/CI)
    return 0 if download_stats['failed'] == 0 else 1

if __name__ == "__main__":
    try:
        # Run the main function and handle exceptions gracefully
        exit_code = main()
        sys.exit(exit_code)
        
    except KeyboardInterrupt:
        # If the user presses Ctrl+C (KeyboardInterrupt), stop gracefully
        print("\n[INTERRUPTED] Process cancelled by user")
        sys.exit(130)
        
    except Exception as e:
        # Catch-all for any other unexpected errors
        print(f"\n[FATAL ERROR] {e}")
        
        # Print the full traceback for debugging
        import traceback
        traceback.print_exc()
        
        # Exit with error code 1 (generic failure)
        sys.exit(1)

# End of fetch_itrdb_ca_noaa.py