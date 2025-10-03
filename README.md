# ITRDB Canada Tree-Ring Pipeline

Tools and notebooks for assembling a clean dataset of Canadian tree-ring measurements from the NOAA/NCEI International Tree-Ring Data Bank (ITRDB) and exploring links with ERA5 climate drivers.

---

## Project Goals
- Mirror the ITRDB Canada archive locally with resilient HTTP crawling.
- Normalize NOAA template metadata and ring-width measurements for analysis.
- Build region-aware summaries and coordinate lookups for climate joins.
- Prototype growth–climate relationships (e.g., growing degree days) in notebooks.

## Workflow
1. **Fetch NOAA-formatted series** (`*-rwl-noaa.txt`) directly from the public archive using `fetch_itrdb_ca_noaa.py`. The crawler discovers companion metadata, tracks provenance, and can resume partial downloads.
2. **Extract ring-width tables** with `extract_tree_rings.py`. The extractor standardizes units, enforces quality flags, validates coordinates, and stores tidy parquet/CSV outputs alongside an enriched manifest.
3. **Explore and model** with notebooks in the repo:
   - `EDA.ipynb` — initial manifest checks, spatial summaries, and coverage plots.
   - `ERA5_GDD_TreeGrowth.ipynb` — link sites to ERA5 gridded temperature data and compute growing degree day metrics.
   - `RB_Copy_of_Optimized_ERA5Land_Oct3_4.ipynb` / `test_DuckDB.ipynb` — experiments with scalable querying and climate retrieval.

Each stage can run independently; artifacts flow through `itrdb_canada_noaa/` (raw) → `ring_width_output_new/` (processed) → `exploration_plots/` & notebooks (analysis).

## Repository Layout
```
.
├── fetch_itrdb_ca_noaa.py       # Multithreaded crawler + manifest builder for NOAA templates
├── extract_tree_rings.py        # Parser/validator for ring widths & metadata harmonization
├── itrdb_canada_noaa/           # Downloaded NOAA-formatted measurement files + manifest CSV
├── ring_width_output_new/       # Processed tidy tables (per-site parquet/CSV, metadata joins)
├── data/                        # Ancillary GIS layers (e.g., Canadian provincial boundaries)
├── exploration_plots/           # Derived figures cached from notebooks (optional)
├── *.ipynb                      # Exploratory data analysis and climate-integration notebooks
├── environment.yml              # Conda environment with geospatial + climate tooling
├── LICENSE
└── README.md                    # You are here
```

## Environment Setup
The project expects Python 3.11 with geospatial and Earth Engine dependencies. Create the Conda environment listed in `environment.yml`:

```bash
mamba env create -f environment.yml
conda activate itrdb-gee
```

If `mamba` is unavailable, replace the first command with `conda env create` (installation takes longer because of heavy geospatial stacks).

## Fetching NOAA Files
The fetcher only downloads NOAA template files (`*-rwl-noaa.txt`) because they embed both metadata and measurements.

```bash
python fetch_itrdb_ca_noaa.py \
  --roots measurements \
  --manifest itrdb_canada_noaa/itrdb_canada_noaa_manifest.csv \
  --data-dir itrdb_canada_noaa \
  --max-workers 16 \
  --resume
```

Key flags:
- `--roots {measurements,chronologies,both}` to pick archive branches.
- `--scan-only` to rebuild the manifest without re-downloading files.
- `--max-workers` controls concurrent requests; tune based on bandwidth.
- `--resume` skips files already fetched completely.

The script emits a manifest CSV with crawl diagnostics, bounding boxes, and metadata quality scores.

## Processing Ring-Width Series
Convert the raw NOAA text files into tidy data and enrich metadata for modeling:

```bash
python extract_tree_rings.py \
  --manifest itrdb_canada_noaa/itrdb_canada_noaa_manifest.csv \
  --data-dir itrdb_canada_noaa \
  --output-dir ring_width_output_new \
  --log-level INFO
```

Highlights:
- Detects measurement units via end-of-series markers and converts to millimetres.
- Drops sentinel values (`-9999`, `999`, etc.) and enforces range checks on years, widths, and coordinates.
- Writes per-site parquet/CSV files plus a consolidated `ring_width_index.parquet` (if enabled) for quicker notebook loads.
- Produces a JSON summary of skipped files and reason codes for QA.

## Notebooks & Analysis
Notebooks assume the processed outputs are present. They rely on `geopandas`, `rioxarray`, and Earth Engine libraries from the Conda environment.

Typical flow:
1. Load the manifest + processed ring widths.
2. Join against shapefiles in `data/` for provincial summaries.
3. Query ERA5 Land or ERA5-Land Monthly, compute climate indicators, and visualize correlations.

Generated plots can be cached under `exploration_plots/`; leaving the directory empty is fine.

## Data & Storage Notes
- `itrdb_canada_noaa/` can grow beyond 1 GB; consider pruning unused provinces.
- The shapefile in `data/` (`lpr_000b16a_e.*`) provides Canadian census divisions and is used for spatial joins.
- Outputs in `ring_width_output_new/` are safe to delete and regenerate.

## Contributing / Next Steps
- Automate ERA5 downloads (currently notebook-driven) into a reproducible script.
- Add tests for `extract_tree_rings.py` parsing edge cases and unit conversions.
- Extend manifests with chronologies once validation rules are finalized.

Feel free to open issues or PRs when you spot data-quality problems or want to add new climate integrations.