# ITRDB Canada Tree-Ring Fetcher

This repository provides a Python tool to **crawl, parse, and download tree-ring measurement files** from the [NOAA/NCEI International Tree-Ring Data Bank (ITRDB)](https://www.ncei.noaa.gov/products/paleoclimatology/tree-ring).  
It builds a **clean manifest** of metadata (site, species, years, lat/lon, elevation, province) and can also **download** the files for local use.

---

## Why not use the NOAA API?

The official **NOAA Paleo Search API** (`/paleo-search/data/search.json`) is currently **down/unreliable**.  

Instead, this tool:

- Crawls the **ITRDB Canada directory structure directly** via HTTPS. [Link here](https://www.ncei.noaa.gov/pub/data/paleo/treering/measurements/northamerica/canada/).
- Reads NOAA’s own **template metadata files** (`*-noaa.txt`, `*-rwl-noaa.txt`).
- Matches each `.rwl/.rwt` measurement file with the correct companion template using the `# Related_Online_Resource` link inside headers.
- Extracts robust metadata without relying on the API.

This makes the fetcher more **resilient** and ensures access to Canadian datasets even when the API is unavailable.

---

## Features

- Smart directory **crawl** of ITRDB Canada (measurements + chronologies).
- **Remote-aware companion detection** for NOAA metadata templates.
- **Robust coordinate parsing**:
  - NOAA bounds midpoints,
  - Tucson compact format (`5035-12248` → 50.35, -122.48),
  - DMS and decimal fallbacks.
- **Province detection** (pattern, alias, bounding box).
- **Quality scoring** (0–100) based on metadata completeness.
- **Manifest CSV** output with normalized fields.
- Optional **parallel downloads with resume**.

---

## Installation

Clone the repo and create the environment:

```bash
git clone https://github.com/<your-org>/ITRDB-TREE-RINGS.git
cd ITRDB-TREE-RINGS
conda env create -f environment.yml
conda activate itrdb-canada
```

## Usage

Scan only (build manifest, no downloads):

```bash
python fetch_itrdb_ca.py --province all --roots measurements --scan-only
```

Download with resume:

```bash
python fetch_itrdb_ca.py --province bc,ab --roots both --resume --max-workers 16
```

Key options:

- --province: comma-separated list (bc,ab,sk,...) or all.
- --roots: measurements, chronologies, or both.
- --scan-only: build manifest only, skip downloads.
- --resume: continue partial .part downloads.
- --small-first: prioritize small files.
- --max-size-mb: skip oversized files.
- --verbose: debug logging

## Outputs

1. **Manifest CSV** (default: `itrdb_canada_manifest.csv`):
   Includes `site_code`, `site_name`, `species_code`, `lat/lon`, `elevation_m`, `years`, `province`, `quality_score`, `detection_method`, etc.

2. **Downloaded files** (if not `--scan-only`):
   Saved under:

   ```text
   ./itrdb_canada/<PROVINCE>/<root>/<relative_path_from_NOAA>/
   ```

3. **Console reports**:

- Quality distribution (excellent/good/fair/poor).
- Metadata availability (with vs. without NOAA companion).
- Top species codes.
- Per-province summary (total, high quality, coords, species coverage).