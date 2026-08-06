# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PySpark + Apache Sedona + GDAL pipelines that ingest and transform Brazilian public-data
sources into a medallion data lake. Each source is an **app** under `apps/` (`anac`, `datasus`,
`ibge`, `tse`, `bcb`, `caged`, `cvm`, `dnit`, `inmet`, `snis`, `ana`, `educacao`). This is a
validation/development environment: sources are self-contained and jobs read/write an
S3-compatible object store (MinIO) over the S3A connector.

## Read first

**`CODING_STANDARDS.md` is the authoritative PySpark style guide** (adapted Palantir guide) and is
non-negotiable for any job or util change. Key rules that shape the code:
- Code/identifiers/column names in **English**; docstrings and comments in **Portuguese**.
- Model each step as a pure `DataFrame -> DataFrame` or `Column -> Column` function; chain with
  `DataFrame.transform(...)`. **Reuse `utils/` before writing new generic transforms** — new generic
  ones belong in `utils/`, not in the job body.
- **No UDFs.** Prefer native `F.<fn>(...)` over `F.expr("...")`. Prefer the Sedona Python API over
  hand-built spatial SQL.
- Absence is `F.lit(None)`, never `""`/`"NA"`/`0`; don't null→0 before aggregating.
- Explicit schemas (no `inferSchema`); latitude/longitude always `double`.
- Always set `how=` on joins; never use `dropDuplicates`/`distinct` to paper over unexpected fan-out.
- Window functions always need an explicit frame (`rowsBetween`/`rangeBetween`).
- Import aliases are fixed: `from pyspark.sql import types as T, functions as F`.

## Architecture

**Medallion layers** map to directories: `apps/<app>/jobs/{bronze,silver,gold}/<table>.py`. Each job
file has a `main()` that builds a Spark (or Sedona) session, reads the previous layer, transforms,
and **persists** the result — one job = one output table. Run jobs in layer order (a `raw` layer,
ingested externally, precedes `bronze`).
- **bronze** — read raw (CSV/etc.) with an explicit `StructType`, standardize column names to
  `snake_case` via `.alias()` in the `select`, clean/cast with `utils`.
- **silver** — join/consolidate across bronze tables, derive fields, build geometries.
- **gold** — aggregate to analytics tables (per-UF rollups, rankings, H3 hexbins).

**Metadata-driven table locations.** `apps/<app>/metadata.toml` declares every table, its `levels`,
key, and a per-level `schema`. `utils/metadata.py::table_location(app, table, level)` resolves the
S3A path (`s3a://{db}/{app}/{name}/{level}`) and fails fast if the table/level isn't declared. **New
tables and layers must be declared in `metadata.toml`, and jobs should resolve paths via
`table_location` rather than hardcoding `s3a://...`** (many existing jobs still hardcode paths — the
metadata-driven form is the target pattern; follow it in new/edited jobs).

**Shared utils** (`utils/`, importable as `from utils import ...` because `PYTHONPATH` = project root):
- `column_transforms.py` — `Column -> Column` helpers (text/number/date normalization, null-safety,
  arrays/maps/structs, coordinates, Sedona geometry repair, window ffill, surrogate keys).
- `dataframe_transforms.py` — `DataFrame -> DataFrame` helpers (coordinate filtering, point/line
  geometry, reprojection, H3 gridding/aggregation, joins without silent fan-out, window analytics,
  JSON/struct flattening).
- App-specific transforms live in `apps/<app>/utils/column_transforms.py` (e.g. domain validators
  like `is_valid_oaci`).

**Geospatial.** Sedona jobs create the session with `SedonaContext.create(...)`. Geometry output is
written as `geoparquet` in EPSG:3857 (Web Mercator); **distances are geodesic (`ST_DistanceSpheroid`)
measured on the original WGS84 coordinates before projecting** (see the Sedona section of
`CODING_STANDARDS.md`). Brazil coordinate bounds live in `dataframe_transforms.py`.

## Running

Everything runs inside the `spark` Docker service (Fedora 39, Java 11, Python 3.11 venv, with
Sedona/GDAL/S3A jars pre-baked into the image). The compose file joins an **external** Docker network
`external-net` and expects an S3 host (`s3:9000`, MinIO) provided by that network — create the network
and start MinIO out of band before `up`.

```bash
# build image (GDAL compile is ~10 min, cached in its own layer)
docker compose build

# start the long-lived container (it just tails; jobs are run via exec)
docker compose up -d

# run a single job (paths are relative to the mounted project root)
docker compose exec spark spark-submit apps/anac/jobs/bronze/anac_aerodromo.py

# JupyterLab (table exploration) and Spark UI are exposed:
#   Spark UI  -> host :14040  (SPARK_UI_PORT)
#   Jupyter   -> host :18888  (JUPYTER_PORT)
```

Config: `.env` (copy from `.env.example`) supplies S3 endpoint/bucket/credentials. Spark's S3A wiring
lives in `conf/spark-defaults.conf` (read via `SPARK_CONF_DIR`), keeping connection config out of job
code. Credentials come from `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` env vars.

## Testing

There is no automated test suite. Per `CODING_STANDARDS.md`, verify changes by running the affected
job(s) locally against the lake and confirming the output data (row counts, `.show()`) matches
expectations. Because transforms are pure `Column`/`DataFrame` functions, they can also be exercised
in isolation in a Jupyter session.
