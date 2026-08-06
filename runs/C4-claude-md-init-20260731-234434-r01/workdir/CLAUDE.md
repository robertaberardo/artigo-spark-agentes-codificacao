# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A monorepo of **PySpark + Apache Sedona + GDAL** batch pipelines that process Brazilian public-sector open data into a medallion-layered data lake on S3-compatible object storage (MinIO). Each top-level dir under `apps/` is one data source (ANA, ANAC, BCB, CAGED, CVM, DATASUS, DNIT, IBGE, INMET, SNIS, TSE; `educacao` is a scaffold — `metadata.toml` only, no jobs yet).

Comments and docstrings are written in **Brazilian Portuguese**. Match that when editing existing files.

`pyproject.toml` describes this as a "validation environment": Python sources are mounted into the container at runtime, so the build produces a **dependency-only wheel** (`py-modules = []`). Some paths printed by `docker/resources/start.sh` (`validate_setup.py`, `exploration/start_jupyter.sh`) are supplied at runtime and are **not present in this tree** — do not assume they exist.

## Running jobs

Everything runs inside the Docker container defined by `Dockerfile` + `docker-compose.yml`. The container installs GDAL 3.8.1 (compiled from source, ~10 min), a Python 3.11 venv at `/opt/venv`, and pre-downloads the Spark jars (Sedona shaded, geotools-wrapper, `hadoop-aws`, `aws-java-sdk-bundle`) so Spark never resolves them via Ivy at runtime.

```bash
cp .env.example .env                       # S3 creds/endpoint + port mappings
docker network create external-net         # compose expects this external net (MinIO lives here)
docker compose build                       # first build is slow (GDAL compile layer)
docker compose up -d

# Run a single job (spark-submit a job module). WORKDIR is the project root,
# so paths are relative to it:
docker compose exec spark spark-submit apps/<app>/jobs/<layer>/<job>.py
# e.g. apps/ana/jobs/bronze/ana_estacao.py
```

There is **no test suite, linter, or formatter configured** — `pyproject.toml` carries only build/runtime deps (plus a `dev` extra for JupyterLab). "Running a job" is the unit of verification. Spark UI → host port `14040`, Jupyter → `18888` (see `.env.example`).

Key env: `PYTHONPATH=/workspace/spark-project` (so `from utils...` and `from apps...` resolve), `SPARK_CONF_DIR=conf/` (S3A wiring in `conf/spark-defaults.conf`), and `S3_BUCKET` / `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` for the object store.

## Architecture

### Medallion layers as directory structure

Each app is `apps/<app>/jobs/{bronze,silver,gold}/<table>.py`. Data flows **raw → bronze → silver → gold**, one job = one output table:

- **bronze** — `SparkSession`, reads a `raw` CSV (`header=True`, `sep=";"`) with an **explicit `RAW_SCHEMA`** (all `StringType`, matching how data lands), applies column transforms, filters with app-specific validators, writes Parquet `mode("overwrite")`.
- **silver** — consolidation, joins, and geospatial work. Geo jobs create a `SedonaContext` (not a plain `SparkSession`), project geometries to **EPSG:3857** (`to_web_mercator`), and write **`format("geoparquet")`**.
- **gold** — aggregations, rankings (`rank_within_group`), H3 hexbins. Reads silver, writes Parquet.

Every job ends by re-reading its output and printing a row count — keep that idiom.

### metadata.toml is the source of truth for tables and paths

Each app has a `metadata.toml` declaring `[data_lake]` (db/app) and `[tables.<t>]` entries with `levels`, `key`, and a **per-level `schema`** (name/type/description of every column at raw/bronze/silver/gold). This is the contract for what each layer's output looks like — read it before changing a job's columns, and update it alongside schema changes.

`utils/metadata.py` reads it:
- `load_metadata(app)` → parsed dict.
- `table_location(app, table, level)` → `s3a://{db}/{app}/{name}/{level}`, with the bucket overridable by `S3_BUCKET`. It **fails fast** — `KeyError` for an undeclared table, `ValueError` for a level not in `levels`.

**Inconsistency to be aware of:** most jobs derive paths via `table_location(...)`, but ~15 jobs (mostly silver/gold) **hardcode `s3a://datalake/...` string literals** instead. Prefer `table_location` for new code; when editing a hardcoded job, note that its bucket won't respect `S3_BUCKET`.

### Shared vs. app-specific transforms

Reusable, tested-in-practice transforms live in `utils/`, split strictly by return type:
- `utils/column_transforms.py` — functions that take and return a Spark `Column` (expression-level: text/number/date parsing, Brazilian formats like `br_decimal_to_double`/`money_br_to_double`, null handling, arrays/maps/structs, geometry helpers, window expressions, keys).
- `utils/dataframe_transforms.py` — functions that take and return a `DataFrame` (joins, dedup, geospatial ops via Sedona, H3 gridding/aggregation, window features, reprojection).

App-domain logic goes in `apps/<app>/utils/column_transforms.py` (e.g. `ana.utils.is_valid_station_code`). Jobs conventionally alias these as `ct` (shared) and `<app>_ct` (app-specific). **Before writing a new transform, check `utils/` — the shared library is large and deliberately covers common Brazilian-data cleaning cases.**

### Geospatial conventions (Sedona)

- Any function/job using `st_*` requires an **active `SedonaContext`** — a plain `SparkSession` will not have the spatial functions registered.
- `ST_Point(x, y)` is **(longitude, latitude)** — order matters.
- The lake's planar CRS is **EPSG:3857** (Web Mercator, meters); H3 operates in EPSG:4326 and helpers reproject internally.
- Compute geodesic distances (`ST_DistanceSpheroid`) on the **original WGS84 point before** projecting to 3857 — never round-trip 4326→3857→4326 just to measure (precision loss). See the comment in `apps/ana/jobs/silver/ana_estacao_geo.py`.

## Adding a new job

1. Declare the output table (and its per-level schema) in the app's `metadata.toml`.
2. Create `apps/<app>/jobs/<layer>/<name>.py` following the layer idiom above.
3. Resolve input/output paths with `table_location(app, table, level)`.
4. Reuse `utils/` transforms; put source-specific rules in `apps/<app>/utils/`.
5. Verify by `spark-submit`-ing it in the container and checking the printed count.
