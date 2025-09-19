#!/usr/bin/env python3
"""
Optimized ITRDB Canada Fetcher with Integrated Manifest Builder
==============================================================
- Intelligent crawling with enhanced metadata extraction
- Combines NOAA template parsing with Tucson format fallbacks
- Real-time progress tracking with quality scoring
- Comprehensive manifest generation with proper field mapping

Features:
- Enhanced coordinate parsing (decimal, DMS, compact formats)
- Species code normalization and validation
- Province detection with multiple methods
- Companion metadata file processing
- Quality scoring based on data completeness

Usage:
    python fetch_itrdb_ca.py --province all --max-workers 16 --resume
    
Time:
    aprox. ~7 mins (5.5 mins crawling, 1.5ish mins download) with 16 workers
"""

from __future__ import annotations
import argparse, csv, io, re, sys, time, json, hashlib
from dataclasses import dataclass, field
from html import unescape
from pathlib import Path
from typing import Iterable, List, Tuple, Dict, Optional, Set, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict, Counter
import threading
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    class tqdm:
        def __init__(self, iterable=None, total=None, desc=None, unit='it', **kwargs):
            self.iterable = iterable or []
            self.total = total or len(self.iterable) if hasattr(self.iterable, '__len__') else 0
            self.desc = desc or ""
            self.n = 0
            
        def __iter__(self):
            for item in self.iterable:
                yield item
                self.update(1)
            
        def update(self, n=1):
            self.n += n
            if self.n % 50 == 0 or self.n >= self.total:
                print(f"\r{self.desc}: {self.n}/{self.total}", end="", flush=True)
        
        def __enter__(self): return self
        def __exit__(self, *args): print()

BASE = "https://www.ncei.noaa.gov"
ROOTS = {
    "chronologies": "/pub/data/paleo/treering/chronologies/northamerica/canada/",
    "measurements": "/pub/data/paleo/treering/measurements/northamerica/canada/",
}

# Province definitions with enhanced detection patterns
PROVINCES: Dict[str, Dict] = {
    "bc": {
        "name": "British Columbia", 
        "patterns": [r"\bBRITISH\s*COLUMBIA\b", r"\bB\.?\s*C\.?\b", r"\bBC\b"],
        "bbox": (48.0, 60.0, -139.1, -114.0),
        "aliases": ["british columbia", "bc", "b.c.", "columbie-britannique"]
    },
    "ab": {
        "name": "Alberta", 
        "patterns": [r"\bALBERTA\b", r"\bAB\b", r"\bALTA\b"],
        "bbox": (49.0, 60.0, -120.0, -110.0),
        "aliases": ["alberta", "ab", "alta"]
    },
    "sk": {
        "name": "Saskatchewan", 
        "patterns": [r"\bSASKATCHEWAN\b", r"\bSK\b", r"\bSASK\b"],
        "bbox": (49.0, 60.0, -110.0, -101.0),
        "aliases": ["saskatchewan", "sk", "sask"]
    },
    "mb": {
        "name": "Manitoba", 
        "patterns": [r"\bMANITOBA\b", r"\bMB\b", r"\bMAN\b"],
        "bbox": (49.0, 60.4, -102.0, -88.0),
        "aliases": ["manitoba", "mb", "man"]
    },
    "on": {
        "name": "Ontario", 
        "patterns": [r"\bONTARIO\b", r"\bON\b", r"\bONT\b"],
        "bbox": (41.5, 56.9, -95.2, -74.3),
        "aliases": ["ontario", "on", "ont"]
    },
    "qc": {
        "name": "Québec", 
        "patterns": [r"\bQU[ÉE]BEC\b", r"\bQUEBEC\b", r"\bPQ\b", r"\bQC\b", r"\bQUE\b"],
        "bbox": (45.0, 63.7, -80.0, -57.0),
        "aliases": ["quebec", "québec", "qc", "pq", "que"]
    },
    "nb": {
        "name": "New Brunswick", 
        "patterns": [r"\bNEW\s+BRUNSWICK\b", r"\bN\.?\s*B\.?\b", r"\bNB\b"],
        "bbox": (45.2, 48.1, -68.0, -63.7),
        "aliases": ["new brunswick", "nb", "n.b.", "nouveau-brunswick"]
    },
    "ns": {
        "name": "Nova Scotia", 
        "patterns": [r"\bNOVA\s+SCOTIA\b", r"\bN\.?\s*S\.?\b", r"\bNS\b"],
        "bbox": (43.4, 47.1, -66.6, -59.3),
        "aliases": ["nova scotia", "ns", "n.s.", "nouvelle-écosse"]
    },
    "pe": {
        "name": "Prince Edward Island", 
        "patterns": [r"\bPRINCE\s+EDWARD\s+ISLAND\b", r"\bP\.?\s*E\.?\s*I\.?\b", r"\bPEI\b"],
        "bbox": (45.9, 47.2, -64.6, -61.9),
        "aliases": ["prince edward island", "pei", "p.e.i.", "île-du-prince-édouard"]
    },
    "nl": {
        "name": "Newfoundland and Labrador", 
        "patterns": [r"\bNEWFOUNDLAND\b", r"\bLABRADOR\b", r"\bN\.?\s*L\.?\b", r"\bNL\b"],
        "bbox": (46.6, 60.4, -71.5, -52.6),
        "aliases": ["newfoundland", "labrador", "nl", "n.l.", "terre-neuve"]
    },
    "yt": {
        "name": "Yukon", 
        "patterns": [r"\bYUKON\b", r"\bYT\b", r"\bYK\b"],
        "bbox": (59.7, 69.7, -141.1, -123.8),
        "aliases": ["yukon", "yt", "yk"]
    },
    "nt": {
        "name": "Northwest Territories", 
        "patterns": [r"\bNORTHWEST\s+TERRITORIES\b", r"\bN\.?\s*W\.?\s*T\.?\b", r"\bNWT\b"],
        "bbox": (60.0, 78.9, -136.6, -102.0),
        "aliases": ["northwest territories", "nwt", "n.w.t.", "territoires du nord-ouest"]
    },
    "nu": {
        "name": "Nunavut", 
        "patterns": [r"\bNUNAVUT\b", r"\bNU\b"],
        "bbox": (60.0, 84.0, -115.0, -61.0),
        "aliases": ["nunavut", "nu"]
    },
}

# Province token normalization 
PROV_TOKENS = {
    "BRITISH COLUMBIA": "British Columbia",
    "QUEBEC": "Quebec", "QUÉBEC": "Québec", "PQ": "Quebec", "QC": "Quebec",
    "ALBERTA": "Alberta", "SASKATCHEWAN": "Saskatchewan", "MANITOBA": "Manitoba",
    "ONTARIO": "Ontario", "NEW BRUNSWICK": "New Brunswick", "NOVA SCOTIA": "Nova Scotia",
    "PRINCE EDWARD ISLAND": "Prince Edward Island", 
    "NEWFOUNDLAND": "Newfoundland and Labrador", "LABRADOR": "Newfoundland and Labrador",
    "YUKON": "Yukon", "NORTHWEST TERRITORIES": "Northwest Territories",
    "NWT": "Northwest Territories", "NUNAVUT": "Nunavut",
}

# Species code handling 
SPECIES_STOP_CODES = {"CANA","STNDRD","NOAA","NCDC","NCEI","DATA","TREE","RING","BOLD","YEAR","TRSG","ARS","CRNS"}
SPECIES_CODE_NORMALIZE = {"PCMA": "PIMA"}  # Pinus mariana corrections

# Regex patterns
def make_field_regex(label: str) -> re.Pattern:
    return re.compile(rf"(?im)^\s*#\s*{re.escape(label)}\s*:\s*(.+?)\s*$")

# NOAA template field patterns
NOAA_PATTERNS = {
    "site_name": make_field_regex("Site_Name"),
    "study_name": make_field_regex("Study_Name"),
    "location": make_field_regex("Location"),
    "collection_name": make_field_regex("Collection_Name"),
    "species_name": make_field_regex("Species_Name"),
    "common_name": make_field_regex("Common_Name"),
    "species_code": make_field_regex("Tree_Species_Code"),
    "earliest_year": make_field_regex("Earliest_Year"),
    "latest_year": make_field_regex("Most_Recent_Year"),
    "first_year": make_field_regex("First_Year"),
    "last_year": make_field_regex("Last_Year"),
    "elevation": make_field_regex("Elevation"),
    "elevation_m": make_field_regex("Elevation_m"),
    "nlat": make_field_regex("Northernmost_Latitude"),
    "slat": make_field_regex("Southernmost_Latitude"),
    "elon": make_field_regex("Easternmost_Longitude"),
    "wlon": make_field_regex("Westernmost_Longitude"),
}

# Tucson format patterns
TUCSON_PATTERNS = {
    "tucson_line": re.compile(r"^(?:STNDRD|CANA|[0-9]{5,6})\b.*", re.IGNORECASE),
    "compact_coord": re.compile(r"(?<!\d)(\d{2,3})(\d{2})[- ](-?\d{3})(\d{2})(?!\d)"),
    "elevation_m": re.compile(r"(\d{2,5})\s*M\b", re.IGNORECASE),
    "site_line": re.compile(r"(?im)^\s*(?:site\s*(?:code|id)?|code|id)\s*[:=]\s*([A-Za-z0-9._\-]{2,})\s*$"),
    "lat_decimal": re.compile(r"(?im)\b(?:lat|latitude)\s*[: ]\s*([\-+]?\d+(?:\.\d+)?)\s*([NS])?\b"),
    "lon_decimal": re.compile(r"(?im)\b(?:lon|longitude)\s*[: ]\s*([\-+]?\d+(?:\.\d+)?)\s*([EW])?\b"),
}

# Additional generic patterns
GENERIC_COORD_PATTERNS = {
    # LAT: 49.123 N  |  LON: 123.456 W  | "Latitude: 49 12 30 N" etc.
    "lat_dms": re.compile(r"(?i)\b(?:lat|latitude)[^0-9\-+]*([\-+]?\d{1,2})[^\d]+(\d{1,2})(?:[^\d]+(\d{1,2}))?\s*([NS])\b"),
    "lon_dms": re.compile(r"(?i)\b(?:lon|longitude)[^0-9\-+]*([\-+]?\d{1,3})[^\d]+(\d{1,2})(?:[^\d]+(\d{1,2}))?\s*([EW])\b"),
    "lat_h":   re.compile(r"(?i)\b(?:lat|latitude)\s*[:=]?\s*([\-+]?\d+(?:\.\d+)?)\s*([NS])\b"),
    "lon_h":   re.compile(r"(?i)\b(?:lon|longitude)\s*[:=]?\s*([\-+]?\d+(?:\.\d+)?)\s*([EW])\b"),
    # Bare decimals that appear as "... Lat 49.123, Lon -123.456 ..." (no hemisphere)
    "lat_dec": re.compile(r"(?i)\b(?:lat|latitude)\s*[:=]?\s*([\-+]?\d+(?:\.\d+)?)\b"),
    "lon_dec": re.compile(r"(?i)\b(?:lon|longitude)\s*[:=]?\s*([\-+]?\d+(?:\.\d+)?)\b"),
}

# More flexible elevation patterns
ELEV_PATTERNS = [
    re.compile(r"(?i)\b(?:elev|elevation|elev\.?)\s*[:=]?\s*([0-9]{2,5})(?:\s*(?:m|meters|metres))?\b"),
    re.compile(r"(?i)\b(?:elev|elevation|elev\.?)\s*\(m\)\s*[:=]?\s*([0-9]{2,5})\b"),
    re.compile(r"(?i)\b(?:elevation_m|elev_m)\s*[:=]?\s*([0-9]{2,5})\b"),
]

# NOAA metadata filename patterns:
#   can674e-rwl-noaa.txt
#   can674e-noaa.txt
#   can674e-rwl-noaa 2.txt
NOAA_META_NAME_RE = re.compile(
    r"""^
        (?P<base>can[a-z0-9]+)          # e.g., can674e
        (?:-rwl)?                       # optional '-rwl'
        -noaa(?:\s+\d+)?\.txt$          # '-noaa' and optional copy number
    """,
    re.IGNORECASE | re.VERBOSE
)

# Pull links out of a NOAA metadata text blob 
RELATED_RWL_RE = re.compile(r"(?im)^\s*#\s*Related_Online_Resource:\s*(\S+\.rwl)\s*$")
DATA_DOWNLOAD_RE = re.compile(r"(?im)^\s*#\s*Data_Download_Resource:\s*(\S+noaa(?:\s+\d+)?\.txt)\s*$")

def safe_stem_base(name: str) -> Optional[str]:
    """
    From 'can674e.rwl' -> 'can674e'
    From 'can673 2.rwl' -> 'can673' (drop copy counter for base match)
    """
    stem = Path(name).stem.lower().replace(" ", "")
    m = re.match(r"(can[a-z0-9]+)", stem)
    return m.group(1) if m else None

def dms_to_decimal(d: Union[str,int,float], m: Union[str,int,float], s: Optional[Union[str,int,float]], hemi: str) -> float:
    d = float(d); m = float(m); s = float(s) if s is not None else 0.0
    val = d + m/60.0 + s/3600.0
    hemi = (hemi or "").upper()
    if hemi in ("S","W"):
        val = -abs(val)
    else:
        val = abs(val)
    return val

@dataclass
class FileEntry:
    root_key: str
    url: str
    relpath: str
    name: str
    is_dir: bool
    size: int = -1
    last_modified: Optional[str] = None

@dataclass
class ProgressTracker:
    total: int = 0
    completed: int = 0
    failed: int = 0
    start_time: float = field(default_factory=time.time)
    last_update: float = field(default_factory=time.time)
    bytes_downloaded: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)
    
    def update(self, success: bool = True, bytes_count: int = 0):
        with self.lock:
            if success:
                self.completed += 1
            else:
                self.failed += 1
            self.bytes_downloaded += bytes_count
            self.last_update = time.time()
    
    @property
    def elapsed_time(self) -> float:
        return time.time() - self.start_time
    
    @property
    def eta(self) -> Optional[float]:
        if self.completed == 0:
            return None
        rate = self.completed / self.elapsed_time
        remaining = self.total - (self.completed + self.failed)
        return remaining / rate if rate > 0 else None
    
    @property
    def throughput_mb_s(self) -> float:
        if self.elapsed_time == 0:
            return 0
        return (self.bytes_downloaded / (1024 * 1024)) / self.elapsed_time

class OptimizedSession:
    def __init__(self, max_retries: int = 5, backoff_factor: float = 1.0):
        self.session = requests.Session()
        
        # Enhanced retry strategy with compatibility handling
        retry_kwargs = {
            'total': max_retries,
            'status_forcelist': [429, 500, 502, 503, 504, 521, 522, 523, 524],
            'backoff_factor': backoff_factor,
            'raise_on_status': False
        }
        
        # Handle urllib3 version compatibility
        try:
            # Try new parameter name first (urllib3 >= 1.26.0)
            retry_kwargs['allowed_methods'] = ["HEAD", "GET", "OPTIONS"]
            retry_strategy = Retry(**retry_kwargs)
        except TypeError:
            # Fall back to old parameter name (urllib3 < 1.26.0)
            retry_kwargs['method_whitelist'] = ["HEAD", "GET", "OPTIONS"]
            retry_strategy = Retry(**retry_kwargs)
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=32,
            pool_maxsize=32,
            pool_block=True
        )
        
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; ITRDB-Fetcher/4.0; Scientific Research)",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache"
        })
    
    def get(self, url: str, **kwargs) -> requests.Response:
        kwargs.setdefault('timeout', (15, 60))
        return self.session.get(url, **kwargs)
    
    def head(self, url: str, **kwargs) -> requests.Response:
        kwargs.setdefault('timeout', (10, 30))
        return self.session.head(url, **kwargs)

# Utility functions (from your manifest builder)
def to_float_safe(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    try:
        return float(str(s).strip())
    except Exception:
        return None

def group1(regex: re.Pattern, text: str) -> Optional[str]:
    m = regex.search(text)
    return m.group(1).strip() if m else None

def ddmm_to_decimal(deg: int, minutes: int, sign: int = +1) -> float:
    return sign * (float(deg) + float(minutes)/60.0)

def parse_compact_coords(text: str) -> Tuple[Optional[float], Optional[float]]:
    m = TUCSON_PATTERNS["compact_coord"].search(text)
    if not m:
        return None, None
    lat_deg, lat_min, lon_deg, lon_min = map(int, m.groups())
    lat = ddmm_to_decimal(lat_deg, lat_min, +1)
    lon = ddmm_to_decimal(abs(lon_deg), lon_min, -1)  # west negative
    return lat, lon

def parse_coordinates_from_noaa_template(text: str) -> Tuple[Optional[float], Optional[float]]:
    nlat = to_float_safe(group1(NOAA_PATTERNS["nlat"], text))
    slat = to_float_safe(group1(NOAA_PATTERNS["slat"], text))
    elon = to_float_safe(group1(NOAA_PATTERNS["elon"], text))
    wlon = to_float_safe(group1(NOAA_PATTERNS["wlon"], text))

    lat = lon = None

    # If both bounds present, take midpoint (common in -noaa.rwl)
    if nlat is not None and slat is not None:
        lat = (nlat + slat) / 2.0
    elif nlat is not None:
        lat = nlat
    elif slat is not None:
        lat = slat

    if elon is not None and wlon is not None:
        # NOAA “Easternmost” can be positive; westernmost is negative in W hemisphere
        # Midpoint robustly handles both
        lon = (elon + wlon) / 2.0
    elif elon is not None:
        lon = elon
    elif wlon is not None:
        lon = wlon

    # Fallbacks
    if lat is None or lon is None:
        # Tucson compact
        clat, clon = parse_compact_coords(text)
        lat = lat if lat is not None else clat
        lon = lon if lon is not None else clon

    # Last resort: generic LAT/LON lines with hemisphere/DMS/decimals
    if lat is None:
        m = GENERIC_COORD_PATTERNS["lat_dms"].search(text)
        if m:
            lat = dms_to_decimal(m.group(1), m.group(2), m.group(3), m.group(4))
        else:
            m = GENERIC_COORD_PATTERNS["lat_h"].search(text) or GENERIC_COORD_PATTERNS["lat_dec"].search(text)
            if m:
                val = float(m.group(1))
                if m.re is GENERIC_COORD_PATTERNS["lat_h"]:
                    hemi = m.group(2).upper()
                    val = -abs(val) if hemi == "S" else abs(val)
                lat = val

    if lon is None:
        m = GENERIC_COORD_PATTERNS["lon_dms"].search(text)
        if m:
            lon = dms_to_decimal(m.group(1), m.group(2), m.group(3), m.group(4))
        else:
            m = GENERIC_COORD_PATTERNS["lon_h"].search(text) or GENERIC_COORD_PATTERNS["lon_dec"].search(text)
            if m:
                val = float(m.group(1))
                if m.re is GENERIC_COORD_PATTERNS["lon_h"]:
                    hemi = m.group(2).upper()
                    val = -abs(val) if hemi == "W" else abs(val)
                lon = val

    return lat, lon

def parse_from_noaa_metadata(text: str) -> Dict[str, Optional[str]]:
    lat, lon = parse_coordinates_from_noaa_template(text)
    site_name = group1(NOAA_PATTERNS["site_name"], text) or group1(NOAA_PATTERNS["study_name"], text)

    # Elevation: try NOAA fields then generic patterns
    elevation = group1(NOAA_PATTERNS["elevation"], text) or group1(NOAA_PATTERNS["elevation_m"], text)
    if not elevation:
        for pat in ELEV_PATTERNS:
            m = pat.search(text)
            if m:
                elevation = m.group(1)
                break

    year_start = group1(NOAA_PATTERNS["earliest_year"], text) or group1(NOAA_PATTERNS["first_year"], text)
    year_end   = group1(NOAA_PATTERNS["latest_year"], text)   or group1(NOAA_PATTERNS["last_year"], text)

    return {
        "site_name": site_name,
        "province": group1(NOAA_PATTERNS["location"], text),
        "species_name": group1(NOAA_PATTERNS["species_name"], text),
        "common_name": group1(NOAA_PATTERNS["common_name"], text),
        "species_code": group1(NOAA_PATTERNS["species_code"], text),
        "elevation_m": elevation,
        "year_start": year_start,
        "year_end": year_end,
        "collection": group1(NOAA_PATTERNS["collection_name"], text),
        "lat": lat,
        "lon": lon,
    }

def parse_coordinates_from_tucson(text: str) -> Tuple[Optional[float], Optional[float]]:
    # 1) Compact like 5430 -12315
    lat, lon = parse_compact_coords(text)
    if lat is not None and lon is not None:
        return lat, lon

    # 2) DMS / hemisphere / bare decimals
    # Try hemisphere with decimals first (LAT: 49.12 N)
    mlat = GENERIC_COORD_PATTERNS["lat_h"].search(text) or GENERIC_COORD_PATTERNS["lat_dms"].search(text) or GENERIC_COORD_PATTERNS["lat_dec"].search(text)
    mlon = GENERIC_COORD_PATTERNS["lon_h"].search(text) or GENERIC_COORD_PATTERNS["lon_dms"].search(text) or GENERIC_COORD_PATTERNS["lon_dec"].search(text)

    if mlat and mlat.re is GENERIC_COORD_PATTERNS["lat_dms"]:
        lat = dms_to_decimal(mlat.group(1), mlat.group(2), mlat.group(3), mlat.group(4))
    elif mlat and mlat.re is GENERIC_COORD_PATTERNS["lat_h"]:
        lat = float(mlat.group(1))
        if mlat.group(2).upper() == "S":
            lat = -abs(lat)
    elif mlat:
        lat = float(mlat.group(1))

    if mlon and mlon.re is GENERIC_COORD_PATTERNS["lon_dms"]:
        lon = dms_to_decimal(mlon.group(1), mlon.group(2), mlon.group(3), mlon.group(4))
    elif mlon and mlon.re is GENERIC_COORD_PATTERNS["lon_h"]:
        lon = float(mlon.group(1))
        if mlon.group(2).upper() == "W":
            lon = -abs(lon)
    elif mlon:
        lon = float(mlon.group(1))

    return lat, lon


def extract_species_code(text: str, filename: str) -> Optional[str]:
    """Extract species code with multiple fallback methods."""
    
    # Method 1: NOAA template
    code = group1(NOAA_PATTERNS["species_code"], text)
    if code:
        code = code.strip().upper()
        return SPECIES_CODE_NORMALIZE.get(code, code)
    
    # Method 2: Scan header for 4-letter codes
    header_lines = "\n".join(text.splitlines()[:120])
    for match in re.finditer(r"\b([A-Z]{4})\b", header_lines):
        code = match.group(1)
        if code not in SPECIES_STOP_CODES:
            return SPECIES_CODE_NORMALIZE.get(code, code)
    
    # Method 3: Extract from filename
    stem = Path(filename).stem
    filename_match = re.search(r'\b([A-Z]{4})\b', stem.upper())
    if filename_match:
        code = filename_match.group(1)
        if code not in SPECIES_STOP_CODES:
            return SPECIES_CODE_NORMALIZE.get(code, code)
    
    return None

def parse_tucson_header_lines(text: str) -> Dict[str, Optional[str]]:
    out = {
        "site_name": None, "site_code": None, "province": None,
        "elevation_m": None, "year_start": None, "year_end": None
    }

    site_match = TUCSON_PATTERNS["site_line"].search(text)
    if site_match:
        out["site_code"] = site_match.group(1).strip()

    # Look anywhere in the header for elevation variants
    if out["elevation_m"] is None:
        for pat in ELEV_PATTERNS:
            m = pat.search(text)
            if m:
                out["elevation_m"] = m.group(1)
                break

    # keep existing logic (province/site_name/years) but on more header
    lines = [ln.strip() for ln in text.splitlines()[:200]]  # scan a bit more
    for line in lines:
        line_upper = line.upper()

        if out["province"] is None:
            for token, normalized in PROV_TOKENS.items():
                if token in line_upper:
                    out["province"] = normalized
                    break

        if out["site_name"] is None:
            site_match = re.search(r'\b([A-Z][A-Z0-9\' .-]{3,})\b(?:,\s*(?:BRITISH COLUMBIA|QUEBEC|QUÉBEC|ALBERTA|ONTARIO|CANADA))?', line_upper)
            if site_match:
                potential_site = site_match.group(1).title()
                exclude_terms = ["CANADA", "QUEBEC", "QUÉBEC", "BRITISH COLUMBIA", "ALBERTA", "ONTARIO"]
                if not any(term in potential_site.upper() for term in exclude_terms):
                    out["site_name"] = potential_site

        if out["year_start"] is None or out["year_end"] is None:
            years = re.findall(r'(?<!\d)(\d{3,4})(?!\d)', line)
            if len(years) >= 2:
                if out["year_start"] is None:
                    out["year_start"] = years[-2]
                if out["year_end"] is None:
                    out["year_end"] = years[-1]

    return out

def parse_from_noaa_metadata(text: str) -> Dict[str, Optional[str]]:
    """Parse metadata from NOAA template file."""
    lat, lon = parse_coordinates_from_noaa_template(text)
    
    # Prefer Site_Name, fallback to Study_Name
    site_name = group1(NOAA_PATTERNS["site_name"], text) or group1(NOAA_PATTERNS["study_name"], text)
    
    # Elevation: try both patterns
    elevation = group1(NOAA_PATTERNS["elevation"], text) or group1(NOAA_PATTERNS["elevation_m"], text)
    
    # Years: try both patterns
    year_start = group1(NOAA_PATTERNS["earliest_year"], text) or group1(NOAA_PATTERNS["first_year"], text)
    year_end = group1(NOAA_PATTERNS["latest_year"], text) or group1(NOAA_PATTERNS["last_year"], text)
    
    return {
        "site_name": site_name,
        "province": group1(NOAA_PATTERNS["location"], text),
        "species_name": group1(NOAA_PATTERNS["species_name"], text),
        "common_name": group1(NOAA_PATTERNS["common_name"], text),
        "species_code": group1(NOAA_PATTERNS["species_code"], text),
        "elevation_m": elevation,
        "year_start": year_start,
        "year_end": year_end,
        "collection": group1(NOAA_PATTERNS["collection_name"], text),
        "lat": lat,
        "lon": lon,
    }

def parse_from_measurement_header(text: str, filename: str) -> Dict[str, Optional[str]]:
    """Parse metadata from measurement file header."""
    # Parse Tucson header
    tucson_data = parse_tucson_header_lines(text)
    
    # Add coordinates
    lat, lon = parse_coordinates_from_tucson(text)
    tucson_data["lat"] = lat
    tucson_data["lon"] = lon
    
    # Add species code
    tucson_data["species_code"] = extract_species_code(text, filename)
    
    # Site code fallback to filename
    if not tucson_data.get("site_code"):
        stem = Path(filename).stem
        if len(stem) >= 3:
            tucson_data["site_code"] = stem
    
    return tucson_data

def find_companion_metadata_file_remote(entry: FileEntry, files_by_dir_index: Dict, session: OptimizedSession, read_bytes: int = 16384) -> Optional[str]:
    """
    Return RELATIVE PATH (within root) to the companion NOAA metadata file
    for the given measurement entry (.rwl/.rwt). We use:
        1) name-base match (supports '-rwl-noaa.txt' and '-noaa.txt', plus copies ' 2', ' 3')
        2) tie-break by opening candidates and checking '# Related_Online_Resource: .../<this>.rwl'
    """
    # Only for measurement files
    low = entry.name.lower()
    if not (low.endswith(".rwl") or low.endswith(".rwt")):
        return None

    # Figure out directory in crawl index and current measurement base/stem
    rel_dir = str(Path(entry.relpath).parent).replace("\\", "/")
    present = files_by_dir_index.get((entry.root_key, rel_dir), [])
    meas_base = safe_stem_base(entry.name)  # e.g., 'can674e'
    if not meas_base:
        return None

    # Collect candidates in the same directory that look like NOAA metadata for this base
    candidates = []
    for fname in present:
        m = NOAA_META_NAME_RE.match(fname)
        if not m:
            continue
        if m.group("base").lower() == meas_base:
            candidates.append(fname)

    if not candidates:
        # As a fallback, accept any NOAA metadata in this folder if there is exactly one
        any_noaa = [f for f in present if NOAA_META_NAME_RE.match(f)]
        if len(any_noaa) == 1:
            return f"{rel_dir}/{any_noaa[0]}".lstrip("/")
        return None

    # If only one, we’re done
    if len(candidates) == 1:
        return f"{rel_dir}/{candidates[0]}".lstrip("/")

    # Tie-break: open each candidate and check Related_Online_Resource
    # We want it to contain '/<meas_stem>.rwl' that matches this measurement (incl. copies)
    meas_stem = Path(entry.name).name.lower()  # e.g., 'can674e.rwl' or 'can673 2.rwl'
    # Normalize copy suffix for comparisons (strip spaces in stem for robust match)
    meas_stem_comp = meas_stem.replace(" ", "")

    for fname in candidates:
        try:
            url = f"{BASE}{ROOTS[entry.root_key]}{rel_dir}/{fname}"
            resp = session.get(url, headers={'Range': f'bytes=0-{read_bytes-1}'})
            txt = (resp.text if resp.status_code in (200, 206) else read_text_safe(url, session)) or ""
            # Prefer exact match of the related .rwl (ignoring spaces like in 'can673 2.rwl')
            m = RELATED_RWL_RE.search(txt)
            if m:
                related_url = m.group(1).strip().lower()
                related_name = Path(urlparse(related_url).path).name.lower().replace(" ", "")
                if related_name == meas_stem_comp:
                    return f"{rel_dir}/{fname}".lstrip("/")
        except Exception:
            continue

    # If none matched by Related_Online_Resource, pick the first deterministically
    return f"{rel_dir}/{sorted(candidates)[0]}".lstrip("/")


def normalize_province_name(raw_province: Optional[str], folder_name: str) -> str:
    """Normalize province name with fallback to folder."""
    if raw_province:
        upper_raw = raw_province.upper()
        for token, normalized in PROV_TOKENS.items():
            if token in upper_raw:
                return normalized
    
    # Fallback to folder mapping
    folder_upper = folder_name.upper()
    folder_map = {
        "BC": "British Columbia", "AB": "Alberta", "SK": "Saskatchewan", "MB": "Manitoba",
        "ON": "Ontario", "QC": "Quebec", "NB": "New Brunswick", "NS": "Nova Scotia", 
        "PE": "Prince Edward Island", "NL": "Newfoundland and Labrador", "YT": "Yukon",
        "NT": "Northwest Territories", "NU": "Nunavut",
    }
    return folder_map.get(folder_upper, folder_name)

def detect_province(text: str, coordinates: Tuple[Optional[float], Optional[float]], 
                filename: str, use_bbox: bool = True) -> Optional[str]:
    """Detect province using multiple methods."""
    text_upper = text.upper()
    lat, lon = coordinates
    
    # Method 1: Pattern matching
    for prov_key, prov_info in PROVINCES.items():
        for pattern in prov_info["patterns"]:
            if re.search(pattern, text_upper):
                return prov_key
    
    # Method 2: Alias matching
    for prov_key, prov_info in PROVINCES.items():
        for alias in prov_info["aliases"]:
            if alias.upper() in text_upper:
                return prov_key
    
    # Method 3: Bounding box check
    if use_bbox and lat is not None and lon is not None:
        for prov_key, prov_info in PROVINCES.items():
            min_lat, max_lat, min_lon, max_lon = prov_info["bbox"]
            if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
                return prov_key
    
    return None

def calculate_quality_score(metadata: Dict[str, Optional[str]], has_companion: bool = False) -> int:
    """Calculate quality score based on metadata completeness."""
    score = 0
    
    # Basic identification (40 points)
    if metadata.get('site_code'):
        score += 15
    if metadata.get('site_name'):
        score += 15
    if metadata.get('collection') or metadata.get('collection_id'):
        score += 10
    
    # Species information (25 points)
    if metadata.get('species_code'):
        score += 15
    if metadata.get('species_name'):
        score += 10
    
    # Geographic data (25 points)
    if metadata.get('lat') and metadata.get('lon'):
        score += 20
    if metadata.get('elevation_m'):
        score += 5
    
    # Temporal data (10 points)
    if metadata.get('year_start') and metadata.get('year_end'):
        score += 10
    elif metadata.get('year_start') or metadata.get('year_end'):
        score += 5
    
    # Data source bonuses (10 points)
    if has_companion:
        score += 10
    
    return min(score, 100)

def smart_crawl(session: OptimizedSession, root_key: str, max_depth: int = 3) -> List[FileEntry]:
    """Smart directory crawling with depth control."""
    base_url = f"{BASE}{ROOTS[root_key]}"
    entries = []
    visited = set()
    
    def crawl_recursive(url: str, depth: int) -> List[FileEntry]:
        if depth > max_depth or url in visited:
            return []
        
        visited.add(url)
        try:
            response = session.get(url)
            response.raise_for_status()
            
            # Parse directory listing
            links = re.findall(r'href="([^"]+)"', response.text, re.IGNORECASE)
            local_entries = []
            
            for link in links:
                if link.startswith(('?', '#', '../', './')):
                    continue
                
                full_url = urljoin(url, link)
                name = link.rstrip('/').split('/')[-1]
                is_dir = link.endswith('/')
                
                if full_url.startswith(base_url):
                    relpath = full_url.replace(base_url, '').lstrip('/')
                    entry = FileEntry(
                        root_key=root_key,
                        url=full_url,
                        relpath=relpath,
                        name=name,
                        is_dir=is_dir
                    )
                    local_entries.append(entry)
                    
                    # Recurse into subdirectories
                    if is_dir and depth < max_depth:
                        local_entries.extend(crawl_recursive(full_url, depth + 1))
            
            return local_entries
            
        except Exception as e:
            print(f"[WARNING] Failed to crawl {url}: {e}")
            return []
    
    print(f"[INFO] Smart crawling {root_key} (max depth: {max_depth})...")
    entries = crawl_recursive(base_url if base_url.endswith('/') else base_url + '/', 0)
    
    return entries

def read_text_safe(path_or_url: str, session: OptimizedSession = None) -> str:
    """Safely read text from file path or URL."""
    try:
        if session and path_or_url.startswith('http'):
            response = session.get(path_or_url)
            response.raise_for_status()
            return response.text
        else:
            return Path(path_or_url).read_text(encoding="utf-8", errors="ignore")
    except Exception:
        try:
            if session and path_or_url.startswith('http'):
                response = session.get(path_or_url)
                return response.text
            else:
                return Path(path_or_url).read_text(errors="ignore")
        except Exception:
            return ""

def analyze_file_enhanced(session: OptimizedSession, entry: FileEntry, target_provinces: List[str], args, files_by_dir_index: Dict) -> Optional[Dict]:
    """Enhanced file analysis with remote-aware companion lookup and robust parsing."""
    try:
        file_size = -1
        try:
            head_response = session.head(entry.url)
            if head_response.ok and 'content-length' in head_response.headers:
                file_size = int(head_response.headers['content-length'])
        except Exception:
            pass  # size stays -1 if HEAD fails

        # Skip oversized files (if requested)
        if args.max_size_mb and file_size > 0 and file_size > args.max_size_mb * 1024 * 1024:
            if args.verbose:
                print(f"[SKIP] {entry.name} ({file_size/(1024*1024):.1f}MB > {args.max_size_mb}MB)")
            return None

        try:
            # try ranged first for speed
            resp = session.get(entry.url, headers={'Range': 'bytes=0-16383'})
            if resp.status_code in (200, 206):
                header_content = resp.content[:16384].decode('utf-8', errors='ignore')
            else:
                resp = session.get(entry.url)
                resp.raise_for_status()
                header_content = resp.content[:16384].decode('utf-8', errors='ignore')
        except Exception:
            return None

        noaa_metadata: Dict[str, Optional[str]] = {}
        tucson_metadata: Dict[str, Optional[str]] = {}
        has_companion = False
        companion_file_path = ""

        # Determine type & parse + remote-aware companion 
        name_lower = entry.name.lower()

        if name_lower.endswith('-rwl-noaa.txt'):
            # This IS a NOAA companion file; parse it directly
            noaa_metadata = parse_from_noaa_metadata(header_content)
            has_companion = True
            companion_file_path = entry.relpath

        elif name_lower.endswith(('.rwl', '.rwt')):
            tucson_metadata = parse_from_measurement_header(header_content, entry.name)

            companion_rel = find_companion_metadata_file_remote(entry, files_by_dir_index, session)
            if companion_rel:
                try:
                    companion_url = f"{BASE}{ROOTS[entry.root_key]}{companion_rel}"
                    companion_text = read_text_safe(companion_url, session)
                    if companion_text:
                        noaa_metadata = parse_from_noaa_metadata(companion_text)
                        has_companion = True
                        companion_file_path = companion_rel
                except Exception:
                    pass
                
        # Merge metadata (NOAA takes precedence)
        merged_metadata: Dict[str, Optional[str]] = {}

        for field in [
            'site_name', 'species_name', 'common_name', 'species_code',
            'elevation_m', 'year_start', 'year_end', 'province'
        ]:
            nval = noaa_metadata.get(field) if noaa_metadata else None
            tval = tucson_metadata.get(field) if tucson_metadata else None
            merged_metadata[field] = nval if nval not in (None, "") else tval

        # Coordinates: NOAA preferred, fallback to Tucson
        noaa_lat, noaa_lon = (noaa_metadata.get('lat'), noaa_metadata.get('lon')) if noaa_metadata else (None, None)
        tucson_lat, tucson_lon = (tucson_metadata.get('lat'), tucson_metadata.get('lon')) if tucson_metadata else (None, None)
        lat = noaa_lat if noaa_lat is not None else tucson_lat
        lon = noaa_lon if noaa_lon is not None else tucson_lon
        merged_metadata['lat'] = lat
        merged_metadata['lon'] = lon

        # Collection ID: from NOAA collection, else infer from filename (e.g., 'cana096')
        collection = (noaa_metadata.get('collection') if noaa_metadata else None)
        if not collection:
            m = re.search(r'\b(can[a-z0-9]+)\b', entry.name, re.IGNORECASE)
            if m:
                collection = m.group(1).upper()
        merged_metadata['collection_id'] = collection or ""

        # Site code: from Tucson if present, else filename stem
        site_code = (tucson_metadata.get('site_code') if tucson_metadata else None) or Path(entry.name).stem
        merged_metadata['site_code'] = site_code

        #Province detection (patterns/aliases/bbox)
        companion_text = ""
        if companion_file_path:
            try:
                companion_text = read_text_safe(f"{BASE}{ROOTS[entry.root_key]}{companion_file_path}", session) or ""
            except Exception:
                companion_text = ""
        all_text = header_content + ("\n" + companion_text if companion_text else "")

        detected_province = detect_province(all_text, (lat, lon), entry.name, args.use_bbox)

        # If we can't detect, or it's not in the requested set, skip
        if not detected_province or detected_province not in target_provinces:
            if args.verbose:
                print(f"[DEBUG] {entry.name}: detected_province={detected_province} not in targets")
            return None

        # Determine detection method for audit
        detection_method = 'pattern'
        try:
            province_patterns_found = any(
                re.search(pat, all_text.upper()) for pat in PROVINCES[detected_province]['patterns']
            )
        except Exception:
            province_patterns_found = False

        if province_patterns_found:
            detection_method = 'pattern'
        elif (lat is not None) and (lon is not None):
            detection_method = 'bbox'
        else:
            detection_method = 'folder'

        # Build manifest row
        result = {
            'province': normalize_province_name(merged_metadata.get('province'), detected_province),
            'root': entry.root_key,
            'site_code': merged_metadata.get('site_code', '') or '',
            'site_name': merged_metadata.get('site_name', '') or '',
            'collection_id': merged_metadata.get('collection_id', '') or '',
            'species_code': merged_metadata.get('species_code', '') or '',
            'species_name': merged_metadata.get('species_name', '') or '',
            'common_name': merged_metadata.get('common_name', '') or '',
            'lat': f"{lat:.6f}" if isinstance(lat, float) else (str(lat) if lat else ""),
            'lon': f"{lon:.6f}" if isinstance(lon, float) else (str(lon) if lon else ""),
            'elevation_m': merged_metadata.get('elevation_m', '') or '',
            'year_start': merged_metadata.get('year_start', '') or '',
            'year_end': merged_metadata.get('year_end', '') or '',
            'has_meta_txt': 'yes' if has_companion else 'no',
            'meta_txt_file': companion_file_path,
            'ext': entry.name.split('.')[-1].lower() if '.' in entry.name else '',
            'path': entry.relpath,
            'filename': entry.name,

            # Enhanced tracking
            'province_key': detected_province,
            'province_name': PROVINCES[detected_province]['name'],
            'file_size_bytes': file_size,
            'detection_method': detection_method,
            'download_status': 'pending',
            'quality_score': calculate_quality_score(merged_metadata, has_companion),
            'checksum': hashlib.md5(entry.url.encode()).hexdigest()[:8],
            'url': entry.url,
        }

        return result

    except Exception as e:
        if args.verbose:
            print(f"[ERROR] Analysis failed for {entry.name}: {e}")
        return None


def download_with_progress(session: OptimizedSession, entry: FileEntry, 
                        output_path: Path, resume: bool = True) -> Tuple[bool, int]:
    """Download file with resume support."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_suffix(output_path.suffix + '.part')
    
    # Check for resume
    start_byte = 0
    if resume and temp_path.exists():
        start_byte = temp_path.stat().st_size
    
    headers = {}
    if start_byte > 0:
        headers['Range'] = f'bytes={start_byte}-'
    
    try:
        with session.get(entry.url, headers=headers, stream=True) as response:
            if response.status_code not in (200, 206):
                return False, 0
            
            mode = 'ab' if start_byte > 0 else 'wb'
            bytes_written = start_byte
            
            with open(temp_path, mode) as f:
                for chunk in response.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)
                        bytes_written += len(chunk)
            
            # Atomic rename
            temp_path.rename(output_path)
            return True, bytes_written
            
    except Exception as e:
        print(f"[ERROR] Failed to download {entry.name}: {e}")
        return False, 0

def write_manifest(output_path: Path, results: List[Dict]) -> None:
    """Write manifest CSV with proper field mapping."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Use the same field order as your original manifest builder
    fieldnames = [
        'province', 'root', 'site_code', 'site_name', 'collection_id',
        'species_code', 'species_name', 'common_name',
        'lat', 'lon', 'elevation_m', 'year_start', 'year_end',
        'has_meta_txt', 'meta_txt_file', 'ext', 'path', 'filename',
        # Additional enhanced fields
        'province_key', 'province_name', 'file_size_bytes', 'detection_method',
        'download_status', 'quality_score', 'checksum', 'url'
    ]
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            # Only write fields that exist in fieldnames
            filtered_result = {k: result.get(k, '') for k in fieldnames}
            writer.writerow(filtered_result)

def main():
    parser = argparse.ArgumentParser(
        description="Optimized ITRDB Canada Fetcher with Integrated Manifest Builder",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--province', default='all', 
                    help='Province keys (comma-separated) or "all"')
    parser.add_argument('--output', default='./itrdb_canada',
                    help='Output directory')
    parser.add_argument('--roots', default='measurements',
                    choices=['measurements', 'chronologies', 'both'],
                    help='Data types to fetch')
    parser.add_argument('--extensions', default='rwl,txt',
                    help='File extensions to download (comma-separated)')
    parser.add_argument('--max-workers', type=int, default=16,
                    help='Maximum concurrent workers')
    parser.add_argument('--max-size-mb', type=float,
                    help='Skip files larger than this size (MB)')
    parser.add_argument('--resume', action='store_true',
                    help='Resume partial downloads')
    parser.add_argument('--small-first', action='store_true',
                    help='Download smaller files first')
    parser.add_argument('--use-bbox', action='store_true', default=True,
                    help='Use bounding box for province detection')
    parser.add_argument('--scan-only', action='store_true',
                    help='Only scan and create manifest (no downloads)')
    parser.add_argument('--verbose', action='store_true',
                    help='Verbose output')
    parser.add_argument('--manifest', default='itrdb_canada_manifest.csv',
                    help='Manifest filename')
    parser.add_argument('--max-depth', type=int, default=3,
                    help='Maximum crawl depth')
    
    args = parser.parse_args()
    
    # Parse provinces
    if args.province.lower() == 'all':
        target_provinces = list(PROVINCES.keys())
    else:
        target_provinces = [p.strip().lower() for p in args.province.split(',')]
        invalid = [p for p in target_provinces if p not in PROVINCES]
        if invalid:
            print(f"[ERROR] Invalid provinces: {', '.join(invalid)}")
            return 1
    
    # Parse roots
    if args.roots == 'both':
        roots_to_process = ['measurements', 'chronologies']
    else:
        roots_to_process = [args.roots]
    
    # Parse extensions
    target_extensions = [f'.{ext.strip().lower()}' for ext in args.extensions.split(',')]
    
    print(f"[INFO] Target provinces: {', '.join(p.upper() for p in target_provinces)}")
    print(f"[INFO] Target roots: {', '.join(roots_to_process)}")
    print(f"[INFO] Target extensions: {', '.join(target_extensions)}")
    print(f"[INFO] Output directory: {Path(args.output).resolve()}")
    
    # Initialize session
    session = OptimizedSession()
    
    # Phase 1: Smart crawling
    all_entries = []
    for root_key in roots_to_process:
        print(f"\n[PHASE 1] Crawling {root_key}...")
        entries = smart_crawl(session, root_key, args.max_depth)
        
        # Filter by extension
        file_entries = [
            e for e in entries 
            if not e.is_dir and any(e.name.lower().endswith(ext) for ext in target_extensions)
        ]
        
        all_entries.extend(file_entries)
        print(f"[INFO] Found {len(file_entries)} candidate files in {root_key}")
    
    print(f"\n[INFO] Total candidate files: {len(all_entries)}")
    
    # Build a directory index of all crawled files (remote-aware)
    files_by_dir = defaultdict(list)
    for e in all_entries:
        # rel directory of the entry within its root
        rel_dir = str(Path(e.relpath).parent).replace("\\", "/")
        files_by_dir[(e.root_key, rel_dir)].append(e.name.lower())

    # Phase 2: Enhanced parallel analysis
    print(f"\n[PHASE 2] Analyzing file headers...")
    
    matching_files = []
    progress = ProgressTracker(total=len(all_entries))
    
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        future_to_entry = {
                executor.submit(analyze_file_enhanced, session, entry, target_provinces, args, files_by_dir): entry
                for entry in all_entries
            }

        with tqdm(total=len(all_entries), desc="Analyzing headers", 
                unit="files", disable=not HAS_TQDM) as pbar:
            
            for future in as_completed(future_to_entry):
                result = future.result()
                if result:
                    matching_files.append(result)
                
                progress.update(success=result is not None)
                pbar.update(1)
                
                # Enhanced progress info
                if HAS_TQDM and progress.completed > 0:
                    eta_str = f"{progress.eta:.0f}s" if progress.eta else "∞"
                    high_quality = sum(1 for r in matching_files if r.get('quality_score', 0) >= 80)
                    pbar.set_postfix({
                        'matches': len(matching_files),
                        'high_qual': high_quality,
                        'rate': f"{progress.completed/progress.elapsed_time:.1f}/s",
                        'ETA': eta_str
                    })
    
    print(f"\n[INFO] Found {len(matching_files)} matching files")

    # Enhanced statistics
    quality_stats = defaultdict(int)
    species_stats = defaultdict(int)
    companion_stats = {'with_meta': 0, 'without_meta': 0}
    
    for result in matching_files:
        quality = result.get('quality_score', 0)
        if quality >= 80:
            quality_stats['excellent'] += 1
        elif quality >= 60:
            quality_stats['good'] += 1
        elif quality >= 40:
            quality_stats['fair'] += 1
        else:
            quality_stats['poor'] += 1
        
        if result.get('species_code'):
            species_stats[result['species_code']] += 1
        
        if result.get('has_meta_txt') == 'yes':
            companion_stats['with_meta'] += 1
        else:
            companion_stats['without_meta'] += 1
    
    print(f"\n[QUALITY DISTRIBUTION]:")
    print(f"  Excellent (≥80): {quality_stats['excellent']}")
    print(f"  Good (60-79):    {quality_stats['good']}")
    print(f"  Fair (40-59):    {quality_stats['fair']}")
    print(f"  Poor (<40):      {quality_stats['poor']}")
    
    print(f"\n[METADATA AVAILABILITY]:")
    print(f"  With NOAA metadata files:    {companion_stats['with_meta']}")
    print(f"  Header-only parsing:         {companion_stats['without_meta']}")
    
    if species_stats:
        print(f"\n[TOP SPECIES CODES]:")
        for species, count in sorted(species_stats.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {species}: {count}")
    
    # Sort files for optimal download order
    if args.small_first:
        matching_files.sort(key=lambda x: (
            x.get('file_size_bytes', float('inf')) if x.get('file_size_bytes', -1) > 0 else float('inf'),
            -x.get('quality_score', 0),  # Higher quality first within same size
            x.get('province_key', ''),
            x.get('filename', '')
        ))
        print("[INFO] Sorted files by size (smallest first, highest quality within size)")
    else:
        matching_files.sort(key=lambda x: (
            x.get('province_key', ''), 
            x.get('root', ''),
            -x.get('quality_score', 0),  # Higher quality first within province
            x.get('filename', '')
        ))
        print("[INFO] Sorted files by province and quality")
    
    # Write manifest
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / args.manifest
    
    write_manifest(manifest_path, matching_files)
    print(f"[INFO] Manifest written: {manifest_path}")
    
    # Enhanced statistics by province
    province_stats = defaultdict(lambda: {'total': 0, 'high_quality': 0, 'with_coords': 0, 'with_species': 0})
    for result in matching_files:
        pkey = result.get('province_key', '')
        province_stats[pkey]['total'] += 1
        if result.get('quality_score', 0) >= 80:
            province_stats[pkey]['high_quality'] += 1
        if result.get('lat') and result.get('lon'):
            province_stats[pkey]['with_coords'] += 1
        if result.get('species_code'):
            province_stats[pkey]['with_species'] += 1
    
    print(f"\n[DETAILED STATISTICS BY PROVINCE]:")
    print(f"{'Prov':<4} {'Total':<6} {'HiQual':<7} {'Coords':<7} {'Species':<8} {'Province Name'}")
    print("-" * 65)
    for prov_key in sorted(province_stats.keys()):
        stats = province_stats[prov_key]
        prov_name = PROVINCES.get(prov_key, {}).get('name', prov_key)
        print(f"{prov_key.upper():<4} {stats['total']:<6} "
            f"{stats['high_quality']:<7} {stats['with_coords']:<7} "
            f"{stats['with_species']:<8} {prov_name}")
    
    if args.scan_only:
        avg_quality = sum(r.get('quality_score', 0) for r in matching_files) / len(matching_files) if matching_files else 0
        print(f"\n[DONE] Scan-only mode completed. Enhanced manifest saved.")
        print(f"[SUMMARY] {len(matching_files)} files analyzed across {len(target_provinces)} provinces")
        print(f"[QUALITY] Average quality score: {avg_quality:.1f}/100")
        return 0
    
    # Phase 3: Downloads
    print(f"\n[PHASE 3] Downloading {len(matching_files)} files...")
    
    download_progress = ProgressTracker(total=len(matching_files))
    download_stats = {'success': 0, 'failed': 0, 'skipped': 0}
    
    def download_task(result_dict: Dict) -> Dict:
        entry = FileEntry(
            root_key=result_dict.get('root', ''),
            url=result_dict.get('url', ''),
            relpath=result_dict.get('path', ''),
            name=result_dict.get('filename', ''),
            is_dir=False,
            size=result_dict.get('file_size_bytes', -1)
        )
        
        # Determine output path: output/PROVINCE/root_type/relpath
        prov_key = result_dict.get('province_key', '').upper()
        output_path = output_dir / prov_key / entry.root_key / entry.relpath
        
        # Skip if already exists and not resuming
        if output_path.exists() and not args.resume:
            result_dict['download_status'] = 'exists'
            download_progress.update(success=True)
            return result_dict
        
        # Download
        success, bytes_downloaded = download_with_progress(session, entry, output_path, args.resume)
        
        result_dict['download_status'] = 'success' if success else 'failed'
        download_progress.update(success=success, bytes_count=bytes_downloaded)
        
        return result_dict
    
    # Parallel downloads with progress tracking
    updated_results = []
    
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        future_to_result = {executor.submit(download_task, result): result for result in matching_files}
        
        with tqdm(total=len(matching_files), desc="Downloading", 
                unit="files", disable=not HAS_TQDM) as pbar:
            
            for future in as_completed(future_to_result):
                result = future.result()
                updated_results.append(result)
                
                # Update statistics
                status = result.get('download_status', '')
                if status == 'success':
                    download_stats['success'] += 1
                elif status == 'failed':
                    download_stats['failed'] += 1
                else:
                    download_stats['skipped'] += 1
                
                pbar.update(1)
                
                # Enhanced progress info
                if HAS_TQDM and download_progress.completed > 0:
                    eta_str = f"{download_progress.eta:.0f}s" if download_progress.eta else "∞"
                    throughput = download_progress.throughput_mb_s
                    
                    pbar.set_postfix({
                        'success': download_stats['success'],
                        'failed': download_stats['failed'],
                        'MB/s': f"{throughput:.1f}",
                        'ETA': eta_str
                    })
    
    # Final manifest update
    write_manifest(manifest_path, updated_results)
    
    # Final summary
    total_mb = download_progress.bytes_downloaded / (1024 * 1024)
    elapsed_mins = download_progress.elapsed_time / 60
    avg_speed = total_mb / (elapsed_mins / 60) if elapsed_mins > 0 else 0
    
    print(f"\n[COMPLETED] Download Summary:")
    print(f"  Total files processed: {len(matching_files)}")
    print(f"  Successfully downloaded: {download_stats['success']}")
    print(f"  Failed downloads: {download_stats['failed']}")
    print(f"  Skipped (existing): {download_stats['skipped']}")
    print(f"  Total data downloaded: {total_mb:.1f} MB")
    print(f"  Total time: {elapsed_mins:.1f} minutes")
    print(f"  Average speed: {avg_speed:.1f} MB/s")
    print(f"  Final manifest: {manifest_path}")
    
    # Quality report
    if updated_results:
        quality_scores = [r.get('quality_score', 0) for r in updated_results]
        avg_quality = sum(quality_scores) / len(quality_scores)
        high_quality = sum(1 for q in quality_scores if q >= 80)
        print(f"\n[QUALITY REPORT]:")
        print(f"  Average quality score: {avg_quality:.1f}/100")
        print(f"  High quality files (≥80): {high_quality}/{len(quality_scores)} ({high_quality/len(quality_scores)*100:.1f}%)")
    
    print(f"\n[SUCCESS] All operations completed! Check {output_dir} for downloaded files.")
    
    return 0 if download_stats['failed'] == 0 else 1

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Process cancelled by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        if "--verbose" in sys.argv:
            import traceback
            traceback.print_exc()
        sys.exit(1)