#!/usr/bin/env python3
"""
NOAA Tree Ring Data Extractor

Extracts tree ring measurements from NOAA format files (*-rwl-noaa.txt)
Prepares coordinates for ERA5 climate data fetching

Features:
- Handles NOAA template format with explicit headers
- Detects measurement units from end-of-series markers
- Treats elevation -999 as NaN
- Creates coordinate lookup for ERA5 integration

# Extract all ring widths
python extract_tree_rings.py \
    --manifest itrdb_canada_noaa/itrdb_canada_noaa_manifest.csv \
    --data-dir itrdb_canada_noaa \
    --output-dir ring_width_output

# Test with limited files
python extract_tree_rings.py \
    --manifest itrdb_canada_noaa/itrdb_canada_noaa_manifest.csv \
    --data-dir itrdb_canada_noaa \
    --output-dir test_output \
    --max-files 10
"""

import pandas as pd
import numpy as np
from pathlib import Path
import re
from typing import Dict, List, Optional, Tuple
import logging
from tqdm import tqdm
import argparse
import json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Data validation constants
MIN_YEAR = 800
MAX_YEAR = 2025
MIN_RING_WIDTH = 0.001  # mm
MAX_RING_WIDTH = 15.0   # mm

# End-of-series markers (values to skip)
END_OF_SERIES_MARKERS = {999, 9999, -9999, 99999, -99999, 0}

def validate_year(year: int) -> bool:
    """Validate if a year is reasonable for tree ring data"""
    return MIN_YEAR <= year <= MAX_YEAR

def validate_ring_width(width: float) -> bool:
    """Validate if a ring width is reasonable"""
    return MIN_RING_WIDTH <= width <= MAX_RING_WIDTH

def validate_coordinates(lat: float, lon: float) -> bool:
    """Validate if coordinates are within reasonable bounds for Canada"""
    return 40.0 <= lat <= 85.0 and -145.0 <= lon <= -50.0

def safe_float_conversion(value_str: str) -> Optional[float]:
    """Safely convert string to float with validation"""
    try:
        value = float(value_str.strip())
        if np.isnan(value) or np.isinf(value): 
            return None
        return value 
    except (ValueError, TypeError): 
        return None

def safe_int_conversion(value_str: str) -> Optional[int]:
    """Safely convert string to int with validation"""
    try:
        return int(value_str.strip())
    except (ValueError, TypeError):
        return None

def parse_elevation(elev_str: str) -> Optional[float]:
    """Parse elevation, treating -999 as NaN"""
    if not elev_str or str(elev_str).strip() == '':
        return None
    try:
        elev = float(elev_str)
        # -999 is a common NaN marker in NOAA files
        if elev == -999 or not (-10 <= elev <= 6000):
            return None
        return elev
    except (ValueError, TypeError):
        return None

def extract_site_code(filename: str, collection_id: str) -> str:
    """Extract site code from filename or collection_id"""
    # Try collection_id first
    if collection_id and collection_id.strip():
        return collection_id.strip()
    
    # Extract from filename (e.g., cana096-rwl-noaa.txt -> cana096)
    match = re.search(r'\b(can[a-z0-9]+)\b', filename, re.IGNORECASE)
    if match:
        return match.group(1).upper()
    
    # Fallback to filename stem
    return Path(filename).stem.split('-')[0].upper()

def detect_units_from_content(text: str) -> Tuple[str, float]:
    """
    Detect measurement units based on end-of-series marker:
    - If contains -9999: units are 0.001 mm (micrometers -> mm)
    - If contains 999: units are 0.01 mm (hundredths of mm)
    
    Returns: (unit_description, conversion_factor)
    """
    # Look for end-of-series markers in the data section
    if re.search(r'\b-9999\b', text):
        return "0.001 mm", 0.001
    elif re.search(r'\b999\b', text):
        return "0.01 mm", 0.01
    else:
        # Default to hundredths of mm
        return "0.01 mm (assumed)", 0.01

# Regex for extracting shortnames from ## lines
# Example line: "## age_CE    Column for years"
# - Starts with "##"
# - Captures the first word (shortname) before the tab
SHORTNAME_RE = re.compile(r'^\s*##\s*([^\t#\s]+)\s*\t')

def extract_noaa_shortnames(lines: List[str]) -> List[str]:
    """
    Look through file lines and collect all variable shortnames 
    from '##' header lines.
    Example shortnames: 'age_CE', 'RVU01AAU_raw'
    """
    names = []
    for ln in lines:
        m = SHORTNAME_RE.match(ln) # see if the line matches the pattern
        # Only consider non-empty, non-description tokens
        if m:
            token = m.group(1).strip() # grab the shortname
            # skip empty values or description-like tokens
            if token and ',' not in token:
                names.append(token)
    return names

def find_explicit_header(lines: List[str]) -> Tuple[Optional[int], Optional[List[str]]]:
    """
    Look for a real tab-delimited header line in the file 
    where the first column is 'age_CE' (the year column).
    Returns:
        (line_number, [list of header column names])
        OR (None, None) if no header found.
    """
    for i, ln in enumerate(lines):
        s = ln.strip()
        # Skip blank lines or comment lines starting with '#'
        if not s or s.startswith('#'):
            continue
        # Look for tab-delimited lines
        if '\t' in s:
            cols = [c.strip() for c in s.split('\t')]
            # Check if first column is 'age_CE' and there are at least 2 columns
            if cols and cols[0] == 'age_CE' and len(cols) >= 2:
                return i, cols
    return None, None

def parse_noaa_file(file_path: Path, site_info: Dict) -> Optional[pd.DataFrame]:
    """
    Parse NOAA template file (*-rwl-noaa.txt) with robust error handling
    
    Strategy:
    1. Prefer explicit header (line starting with 'age_CE' + tabs)
    2. Fallback to '##' shortnames
    3. Detect units from end-of-series markers
    4. Convert measurements using detected unit conversion factor
    5. Validate all data before including
    """
    try:
        # Read the whole file as text (ignore bad characters instead of crashing)
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            lines = content.splitlines()
        
        # Detect units (e.g., 0.01 mm vs 0.001 mm) from special markers in the data block
        units_desc, conversion_factor = detect_units_from_content(content)
        
        # 1) Try to find tab-delimited explicit header
        header_idx, header_cols = find_explicit_header(lines)
        
        if header_idx is not None:
            # If we found a header line, the column names are those header fields
            shortnames = header_cols
            data_start_idx = header_idx + 1 # data starts right after the header
        else:
            # 2) Fallback: use '##' shortname comment lines as column names
            shortnames = extract_noaa_shortnames(lines)
            # Must start with 'age_CE' and have at least one series column
            if not shortnames or shortnames[0] != 'age_CE' or len(shortnames) < 2:
                logger.warning(f"Invalid/missing NOAA structure in {file_path.name}")
                return None
            
            # Find the first *data* line:
            #  - not a comment
            #  - tab-delimited
            #  - first column parses as a valid year
            data_start_idx = None
            for i, ln in enumerate(lines):
                s = ln.strip()
                if not s or s.startswith('#') or '\t' not in s:
                    continue
                fields = [v.strip() for v in s.split('\t')]
                if len(fields) < 2:
                    continue
                y = safe_int_conversion(fields[0])
                if y is not None and validate_year(y):
                    data_start_idx = i
                    break
            
            if data_start_idx is None:
                logger.warning(f"No valid data rows in {file_path.name}")
                return None
        
        # Validate structure: Basic checks
        expected_cols = len(shortnames)
        if shortnames[0] != 'age_CE':
            logger.warning(f"First column is not 'age_CE' in {file_path.name}")
            return None
        
        # Extract tree ID columns (exclude 'age_CE' and any description-like tokens)
        tree_columns = [c for c in shortnames[1:] 
                    if c and c != 'age_CE' and ',' not in c]
        
        if not tree_columns:
            logger.warning(f"No series columns found in {file_path.name}")
            return None
        
        # Parse data rows into a list of dicts
        parsed: List[Dict] = []
        invalid_years = invalid_widths = valid_measurements = 0
        end_markers = {str(m) for m in END_OF_SERIES_MARKERS} # e.g., {'999', '-9999'}
        
        for ln in lines[data_start_idx:]:
            # Skip comments, blanks, or non-tabbed lines
            if not ln or ln.startswith('#') or '\t' not in ln:
                continue
            
            vals = [v.strip() for v in ln.split('\t')]
            
            # Align row width to header width
            if len(vals) < expected_cols:
                vals = vals + [''] * (expected_cols - len(vals))
            elif len(vals) > expected_cols:
                vals = vals[:expected_cols]
            
            # Parse year (column 0): must be valid year
            year = safe_int_conversion(vals[0])
            if year is None or not validate_year(year):
                invalid_years += 1
                continue
            
            # For each tree series column, parse and convert the value
            for idx_col, tree_id in enumerate(tree_columns, start=1):
                if idx_col >= len(vals):
                    continue # safety check
                
                v = vals[idx_col]
                
                # Skip blanks, explicit NAs, or end-of-series markers
                if not v or v.upper() in ('NA', 'NULL', '') or v in end_markers:
                    continue
                
                # Convert to float (raw measurement in NOAA units)
                raw = safe_float_conversion(v)
                if raw is None:
                    continue
                
                # Convert to millimeters (based on earlier unit detection)
                ring_mm = raw * conversion_factor
                
                # Validate converted ring width
                # Keep only plausible ring widths
                if validate_ring_width(ring_mm):
                    parsed.append({
                        'year': year,
                        'tree_id': tree_id,
                        'ring_width_mm': ring_mm,
                        'site_code': site_info['site_code'],
                        'collection_id': site_info.get('collection_id', ''),
                        'site_name': site_info.get('site_name', ''),
                        'location': site_info.get('location', ''),
                        # Attach site metadata for convenience downstream
                        'species_code': site_info.get('species_code', ''),
                        'species_name': site_info.get('species_name', ''),
                        'lat': site_info.get('lat'),
                        'lon': site_info.get('lon'),
                        'elevation_m': site_info.get('elevation_m'),
                        'units': units_desc,
                        'filename': file_path.name
                    })
                    valid_measurements += 1
                else:
                    invalid_widths += 1
        
        # Build DataFrame if we parsed anything
        if parsed:
            logger.info(
                f"Parsed {file_path.name}: {valid_measurements} valid, "
                f"{invalid_years} invalid years, {invalid_widths} invalid widths, "
                f"units: {units_desc}"
            )
            return pd.DataFrame(parsed)
        
        logger.warning(f"No valid measurements in {file_path.name}")
        return None
        
    except Exception as e:
        # Catch-all so one bad file doesn’t crash the whole batch
        logger.error(f"Error parsing {file_path.name}: {e}")
        return None

def extract_ring_widths(
    manifest_path: str,
    data_directory: str,
    output_dir: str = "./ring_width_output",
    chunk_size: int = 50,
    max_files: Optional[int] = None
) -> None:
    """
    Extract tree ring width data from NOAA files
    
    Args:
        manifest_path: Path to manifest CSV from fetcher
        data_directory: Directory containing downloaded NOAA files
        output_dir: Output directory for extracted data
        chunk_size: Files to process before saving chunk
        max_files: Limit number of files (for testing)
        
    Steps:
        1) Read the manifest CSV (list of files + metadata)
        2) Filter to valid, downloaded files with usable coordinates
        3) For each file: parse ring widths -> tidy rows (year, tree_id, ring_width_mm, site info)
        4) Save results in manageable CSV chunks and keep a small summary
        5) Optionally combine chunks and save a sample for quick inspection
    """
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Load manifest CSV produced by fetcher step
    manifest_file = Path(manifest_path)
    if not manifest_file.exists():
        logger.error(f"Manifest file not found: {manifest_path}")
        return
    
    logger.info(f"Loading manifest from {manifest_file}")
    manifest = pd.read_csv(manifest_file)
    logger.info(f"Manifest contains {len(manifest)} files")
    
    # Filter for successfully downloaded files with valid coordinates
    valid_files = manifest[
        (manifest['lat'].notna()) &
        (manifest['lon'].notna()) &
        (manifest['lat'] != '') &
        (manifest['lon'] != '')
    ].copy()
    
    # If we tracked download status, keep only successful downloads
    if 'download_status' in manifest.columns:
        valid_files = valid_files[valid_files['download_status'] == 'success']
        logger.info(f"Filtered by download_status: {len(valid_files)}")
    
    # Double-check coordinates are numeric and in valid ranges
    valid_coords_mask = []
    for _, row in valid_files.iterrows():
        try:
            lat = float(row['lat'])
            lon = float(row['lon'])
            valid_coords_mask.append(validate_coordinates(lat, lon))
        except:
            valid_coords_mask.append(False)
    
    valid_files = valid_files[valid_coords_mask]
    logger.info(f"After coordinate validation: {len(valid_files)}")
    
    # Optionally limit number of files for testing
    if max_files:
        valid_files = valid_files.head(max_files)
        logger.info(f"Limited to {max_files} files for testing")
    
    if len(valid_files) == 0:
        logger.error("No valid files to process!")
        return
    
    # Derive a stable site code (e.g., CANA617) from filename/collection_id
    logger.info("Extracting site codes from filenames...")
    valid_files['site_code'] = valid_files.apply(
        lambda row: extract_site_code(row['filename'], row.get('collection_id', '')), 
        axis=1
    )
    
    logger.info(f"Processing {len(valid_files)} files")
    
    # Create coordinates dataframe (unique lat/lon per site) for ERA5 fetching
    logger.info("Creating coordinates dataframe for ERA5...")
    coords_df = create_coordinates_dataframe(valid_files)
    coords_output = output_path / "tree_ring_coordinates.csv"
    coords_df.to_csv(coords_output, index=False)
    logger.info(f"Saved {len(coords_df)} unique coordinates to {coords_output}")
    
    # Process files in chunks to avoid huge memory spikes
    data_dir = Path(data_directory)
    all_data_chunks = []    # list of chunk filenames
    chunk_num = 0           # current chunk number
    current_chunk = []      # list of DataFrames in current chunk
    
    processed = 0           # successfully processed files
    failed = 0              # files that failed parsing
    error_types = {'parse': 0, 'path': 0, 'other': 0}
    units_distribution = {} # track units used across files
    
    # Iterate with a progress bar (tqdm if available)
    for idx, row in tqdm(valid_files.iterrows(), total=len(valid_files), 
                        desc="Extracting ring widths"):
        
        # Find file (flat structure in output directory)
        file_path = data_dir / row['filename']
        
        if not file_path.exists():
            # file missing on disk (manifest out of date?)
            logger.debug(f"File not found: {file_path}")
            error_types['path'] += 1
            failed += 1
            continue
        
        # Minimal site info passed to the parser (attached to each output row)
        site_info = {
            'site_code': row['site_code'],
            'collection_id': row.get('collection_id', ''),
            'site_name': row.get('site_name', ''),
            'location': row.get('location', ''),
            'species_code': row.get('species_code', ''),
            'species_name': row.get('species_name', ''),
            'lat': float(row['lat']),
            'lon': float(row['lon']),
            'elevation_m': parse_elevation(row.get('elevation_m', ''))
        }
        
        # Parse one NOAA file -> tidy DataFrame or None
        try:
            ring_data = parse_noaa_file(file_path, site_info)
            
            if ring_data is not None and len(ring_data) > 0:
                current_chunk.append(ring_data) # buffer until we hit chunk_size
                processed += 1
                
                # Track which units this file used (string from parser)
                units = ring_data['units'].iloc[0]
                units_distribution[units] = units_distribution.get(units, 0) + 1
            else:
                error_types['parse'] += 1
                failed += 1
                
        except Exception as e:
            # Catch unexpected parse errors so we keep going
            logger.error(f"Unexpected error processing {file_path.name}: {e}")
            error_types['other'] += 1
            failed += 1
        
        # If we filled a chunk, flush it to disk and reset the buffer
        if len(current_chunk) >= chunk_size:
            chunk_df = pd.concat(current_chunk, ignore_index=True)
            chunk_filename = f"ring_widths_chunk_{chunk_num:03d}.csv"
            chunk_df.to_csv(output_path / chunk_filename, index=False)
            logger.info(f"Saved chunk {chunk_num} with {len(chunk_df)} measurements")
            
            all_data_chunks.append(chunk_filename)
            current_chunk = []
            chunk_num += 1
    
    # After the loop, flush any remaining buffered data
    if current_chunk:
        chunk_df = pd.concat(current_chunk, ignore_index=True)
        chunk_filename = f"ring_widths_chunk_{chunk_num:03d}.csv"
        chunk_df.to_csv(output_path / chunk_filename, index=False)
        logger.info(f"Saved final chunk {chunk_num} with {len(chunk_df)} measurements")
        all_data_chunks.append(chunk_filename)
    
    # Build a simple JSON summary describing the extraction run
    summary = {
        'total_files_attempted': len(valid_files),
        'files_processed_successfully': processed,
        'files_failed': failed,
        'error_breakdown': error_types,
        'unique_coordinates': len(coords_df),
        'data_chunk_files': all_data_chunks,
        'units_distribution': units_distribution,
        'validation_settings': {
            'min_year': MIN_YEAR,
            'max_year': MAX_YEAR,
            'min_ring_width_mm': MIN_RING_WIDTH,
            'max_ring_width_mm': MAX_RING_WIDTH
        }
    }
    
    # Optionally combine all chunk CSVs into one big CSV and compute quick stats
    if all_data_chunks:
        logger.info("Combining chunks for quality analysis...")
        all_chunks = []
        for chunk_file in all_data_chunks:
            chunk_df = pd.read_csv(output_path / chunk_file)
            all_chunks.append(chunk_df)
        
        combined_df = pd.concat(all_chunks, ignore_index=True)
        
        # Add high-level metrics to the summary
        summary.update({
            'total_measurements': len(combined_df),
            'unique_sites': combined_df['site_code'].nunique(),
            'unique_trees': combined_df['tree_id'].nunique(),
            'year_range': f"{combined_df['year'].min()} - {combined_df['year'].max()}",
            'ring_width_range_mm': f"{combined_df['ring_width_mm'].min():.4f} - "
                                f"{combined_df['ring_width_mm'].max():.4f}",
            'ring_width_mean_mm': f"{combined_df['ring_width_mm'].mean():.4f}",
            'ring_width_median_mm': f"{combined_df['ring_width_mm'].median():.4f}",
            'species_count': combined_df['species_code'].nunique(),
            'top_species': combined_df['species_code'].value_counts().head(10).to_dict()
        })
        
        # Save the combined dataset if it's not insanely large
        if len(combined_df) < 10_000_000:  # Less than 10M rows
            combined_path = output_path / "all_ring_widths_combined.csv"
            combined_df.to_csv(combined_path, index=False)
            logger.info(f"Saved combined dataset: {combined_path}")
        else:
            logger.info("Combined dataset too large, using chunks only")
        
        # Also save a random sample for quick inspection or plotting
        sample_size = min(50_000, len(combined_df))
        sample_df = combined_df.sample(n=sample_size, random_state=42)
        sample_path = output_path / "ring_widths_sample.csv"
        sample_df.to_csv(sample_path, index=False)
        logger.info(f"Saved sample dataset ({sample_size} measurements): {sample_path}")
    
    # Dump the summary to JSON for reproducibility/reporting
    summary_path = output_path / "extraction_summary.json"
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    # Print final report
    logger.info("\n" + "="*60)
    logger.info("EXTRACTION COMPLETE!")
    logger.info("="*60)
    logger.info(f"Successfully processed: {processed} files")
    logger.info(f"Failed: {failed} files")
    logger.info(f"  - Parse errors: {error_types['parse']}")
    logger.info(f"  - Path errors: {error_types['path']}")
    logger.info(f"  - Other errors: {error_types['other']}")
    logger.info(f"\nUnits distribution:")
    for unit, count in sorted(units_distribution.items()):
        logger.info(f"  {unit}: {count} files")
    logger.info(f"\nUnique coordinates: {len(coords_df)}")
    logger.info(f"Output directory: {output_path}")
    logger.info(f"Coordinates file: {coords_output}")
    logger.info("="*60)

def create_coordinates_dataframe(manifest_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create unique coordinates dataframe for ERA5 climate data fetching
    
    Returns DataFrame with one row per unique lat/lon pair, containing:
    - site_id: unique identifier
    - lat, lon: coordinates
    - elevation_m: elevation (if available)
    - location: province/state
    - year_start, year_end: temporal range
    - site_code: site identifier
    - site_name: site name
    - collection_id: collection identifier
    - species_code: species present at this location
    """
    
    # Validate and prepare coordinates
    valid_coords = []
    for _, row in manifest_df.iterrows():
        try:
            lat = float(row['lat'])
            lon = float(row['lon'])
            if validate_coordinates(lat, lon):
                valid_coords.append(row)
        except:
            continue
    
    if not valid_coords:
        logger.warning("No valid coordinates found")
        return pd.DataFrame()
    
    valid_coords_df = pd.DataFrame(valid_coords)
    
    # Convert to numeric and round for grouping
    valid_coords_df['lat'] = pd.to_numeric(valid_coords_df['lat'], errors='coerce')
    valid_coords_df['lon'] = pd.to_numeric(valid_coords_df['lon'], errors='coerce')
    valid_coords_df['lat_rounded'] = valid_coords_df['lat'].round(6)
    valid_coords_df['lon_rounded'] = valid_coords_df['lon'].round(6)
    
    # Aggregate functions for grouping
    agg_dict = {
        'site_name': lambda x: '; '.join(sorted(set(str(v) for v in x if pd.notna(v) and str(v).strip()))),
        'collection_id': lambda x: '; '.join(sorted(set(str(v) for v in x if pd.notna(v) and str(v).strip()))),
        'species_code': lambda x: '; '.join(sorted(set(str(v) for v in x if pd.notna(v) and str(v).strip()))),
    }
    
    # Add site_code if it exists
    if 'site_code' in valid_coords_df.columns:
        agg_dict['site_code'] = lambda x: '; '.join(sorted(set(str(v) for v in x if pd.notna(v) and str(v).strip())))
    
    # Add location if it exists
    if 'location' in valid_coords_df.columns:
        agg_dict['location'] = 'first'
    
    # Add elevation
    agg_dict['elevation_m'] = lambda x: x.apply(parse_elevation).dropna().mean() if any(pd.notna(x)) else None
    
    # Add year range if available
    if 'year_start' in valid_coords_df.columns:
        agg_dict['year_start'] = lambda x: pd.to_numeric(x, errors='coerce').min()
    if 'year_end' in valid_coords_df.columns:
        agg_dict['year_end'] = lambda x: pd.to_numeric(x, errors='coerce').max()
    
    # Group by unique coordinates
    unique_coords = valid_coords_df.groupby(['lat_rounded', 'lon_rounded']).agg(agg_dict).reset_index()
    
    # Rename and add site_id
    unique_coords = unique_coords.rename(columns={
        'lat_rounded': 'lat',
        'lon_rounded': 'lon'
    })
    unique_coords['site_id'] = [f"site_{i:04d}" for i in range(len(unique_coords))]
    
    # Reorder columns for clarity
    base_cols = ['site_id', 'lat', 'lon', 'elevation_m']
    if 'location' in unique_coords.columns:
        base_cols.append('location')
    if 'year_start' in unique_coords.columns and 'year_end' in unique_coords.columns:
        base_cols.extend(['year_start', 'year_end'])
    if 'site_code' in unique_coords.columns:
        base_cols.append('site_code')
    base_cols.extend(['site_name', 'collection_id', 'species_code'])
    
    # Only include columns that exist
    cols = [c for c in base_cols if c in unique_coords.columns]
    unique_coords = unique_coords[cols]
    
    logger.info(f"Created {len(unique_coords)} unique coordinate pairs")
    return unique_coords

def main():
    """Main function with command line arguments"""
    parser = argparse.ArgumentParser(
        description="Extract tree ring measurements from NOAA files"
    )
    parser.add_argument("--manifest", required=True,
                        help="Path to manifest CSV file from fetcher")
    parser.add_argument("--data-dir", required=True,
                        help="Directory containing downloaded NOAA files")
    parser.add_argument("--output-dir", default="./ring_width_output",
                        help="Output directory for extracted data")
    parser.add_argument("--chunk-size", type=int, default=50,
                        help="Number of files to process before saving chunk")
    parser.add_argument("--max-files", type=int,
                        help="Maximum number of files to process (for testing)")
    
    args = parser.parse_args()
    
    extract_ring_widths(
        manifest_path=args.manifest,
        data_directory=args.data_dir,
        output_dir=args.output_dir,
        chunk_size=args.chunk_size,
        max_files=args.max_files
    )

if __name__ == "__main__":
    main()