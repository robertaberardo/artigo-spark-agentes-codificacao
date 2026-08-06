# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PySpark + Apache Sedona geospatial data pipelines over Brazilian public-data sources
(ANA, ANAC, BCB, CAGED, CVM, DATASUS, DNIT, IBGE, INMET, SNIS, TSE, educação). Jobs
read from and write to an S3-compatible object store (`s3a://`) as a **medallion lake**:
`raw → bronze → silver → gold`. There is no orchestrator — each job is a standalone
`spark-submit` script.

`CODING_STANDARDS.md` is the authoritative PySpark style guide (Palantir-derived, in
Portuguese). **Read it before writing or editing any job or util** — the rules below are
architecture, not a substitute for it. Note the language convention: **code/identifiers/
column names in English; docstrings and comments in Portuguese.**

## Layout

```
apps/<app>/
  metadata.toml            # source of truth: tables, layers, per-layer schema, keys
  jobs/{bronze,silver,gold}/<table>.py   # one job per output table
  utils/column_transforms.py             # column transforms specific to THIS app's domain
utils/                     # SHARED, app-agnostic transforms
  column_transforms.py     # Column -> Column (text, dates, money, arrays, geometry helpers)
  dataframe_transforms.py  # DataFrame -> DataFrame (joins, windows, H3, reproject, flatten)
  metadata.py              # table_location() — resolves s3a paths from metadata.toml
conf/spark-defaults.conf   # S3A connector config (read via SPARK_CONF_DIR)
docker/                    # image build + entrypoint scripts
```

## The three concerns that span multiple files

**1. `metadata.toml` is the schema/path contract.** Every table declares its `levels`
(which of raw/bronze/silver/gold exist), its `key`, and a full column schema *per level*.
Jobs never hardcode paths — they call `table_location(app, table, level)` from
`utils/metadata.py`, which builds `s3a://{S3_BUCKET or db}/{app}/{name}/{level}`. It raises
if the table or level isn't declared, so a new table/layer must be added to `metadata.toml`
first. (A minority of older jobs hardcode `s3a://…` strings — `table_location` is the
canonical form; prefer it for new/edited jobs.)

**2. The medallion flow dictates each layer's shape:**
- **bronze** reads raw CSV with an *explicit* `StructType` (never `inferSchema`),
  `header=True`, `sep=";"`, standardizes columns to `snake_case` via `.alias(...)` in a
  single `select`, applies `utils.column_transforms` (e.g. `normalize_text`, `to_coordinate`,
  `br_decimal_to_double`, `standardize_uf`), filters invalid rows, writes `.parquet`.
  Raw lat/long land as `string` and become `double` here.
- **silver** consolidates/enriches: joins bronze tables, applies window functions
  (moving averages, forward-fill), and does geospatial enrichment with Sedona. Geo silver
  jobs write **geoparquet** (`.format("geoparquet")`); non-geo silver writes parquet.
- **gold** produces analytics-ready aggregates: rankings (`rank_within_group`), per-UF
  rollups, and **H3 hexbin** density grids (`attach_h3_index` + `aggregate_by_h3`).

**3. Reuse before writing.** Generic transforms belong in `utils/`, not job bodies —
`dataframe_transforms.py` already has joins-without-fanout, windowing, H3 gridding, CRS
reprojection, flatten, dedup; `column_transforms.py` has text/date/money/coordinate/array
helpers. Domain-specific column logic (e.g. `is_valid_station_code` for ANA's 8-digit codes)
goes in `apps/<app>/utils/column_transforms.py`. Chain steps with `DataFrame.transform(...)`,
one pure function per step. Check both util modules before adding a new transform.

### Geospatial specifics (Sedona)

- Geo jobs create the session via `SedonaContext.create(SedonaContext.builder()...)`, not a
  plain `SparkSession` — the ST_ functions are registered by that context.
- **Persisted geometry is stored in EPSG:3857** (Web Mercator); `to_web_mercator()` does the
  final reprojection before write.
- **Distances are geodesic on the original WGS84 coordinates** (`ST_DistanceSpheroid`),
  computed *before* reprojecting to 3857 — never round-trip through 3857 just to measure.
  See the extended rationale in `CODING_STANDARDS.md` and the worked example in
  `apps/ana/jobs/silver/ana_estacao_geo.py`.
- `ST_Point(x, y)` is `(longitude, latitude)` — watch the order.

## Running jobs

Everything runs inside the Docker image (Fedora + Java 11 + Python 3.11 venv + GDAL/PROJ +
Spark 3.5.4 / Sedona 1.9.0). The project dir is bind-mounted at `/workspace/spark-project`,
with `PYTHONPATH` set there so `from utils... import` and `from apps.<app>... import` resolve.

```bash
cp .env.example .env                     # S3 endpoint/creds + host port mappings
docker network create external-net       # compose expects an external network named this
docker compose build                      # ~long first build: GDAL compiles (~10 min), cached after
docker compose up -d

# Run one job (path is relative to the mounted project root):
docker compose exec spark spark-submit apps/ana/jobs/bronze/ana_estacao.py

# Interactive shell in the container:
docker compose exec spark bash
```

Jobs are ordered by dependency — a silver/gold job reads the parquet its upstream bronze/
silver job wrote, so run them in `raw → bronze → silver → gold` order for a given app.
Each job prints a `>> <layer> <table>: N registros em <path>` line and calls `spark.stop()`.

The S3A object store (endpoint, bucket, credentials) is external to this repo and provided
via `.env` / `spark-defaults.conf`; there is no local mock in-tree.

## Verifying changes

There is **no test suite and no `validate_setup.py`/`exploration/` in the tree** (the
container banner references them, but they are not present here). Verify a change by running
the affected job with `spark-submit` and confirming the printed row count and that the output
reads back with the expected schema — per `CODING_STANDARDS.md`, run the job or manually
check the data came out as expected.
