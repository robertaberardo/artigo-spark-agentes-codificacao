# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PySpark 3.5.4 + Apache Sedona 1.9.0 + GDAL geospatial data pipelines over Brazilian public-sector datasets (ANAC, DATASUS, IBGE, TSE, DNIT, INMET, BCB, CAGED, CVM, SNIS, ANA, educação). Data lives in an S3A-compatible object store (MinIO, bucket `datalake`) as Parquet / GeoParquet. Everything runs inside a Docker container (Fedora 39, Java 11, Python 3.11) — there is no local Python environment.

**`CODING_STANDARDS.md` is the authoritative PySpark style guide for this repo (Palantir-based, in Portuguese). Read it before writing or reviewing any job or util** — it governs pure-function transforms, `select`-as-schema-contract, null handling, joins, window frames, and Sedona/geo rules, and reviews are expected to enforce it.

## Language convention

Code, function/variable names, and column names are **English**; docstrings and comments are **Portuguese**. Transformed column names (bronze onward) are `snake_case`; the lowercasing happens in the bronze `select` via `.alias(...)`.

## Architecture

### Medallion layers
Each dataset flows raw → bronze → silver → gold. A job is a standalone `spark-submit` script under `apps/<app>/jobs/<layer>/<name>.py` with a `main()` that builds a session, reads, transforms, and **persists** the result.

- **raw** — source CSVs (usually `sep=";"`, header, Brazilian decimals). Read with an **explicit** `StructType` schema, never `inferSchema`. All columns typically typed `string` at this stage.
- **bronze** — one table per raw source: typed, snake_cased, cleaned via `utils` column transforms. Written as Parquet. Lat/long become `double` here (never string).
- **silver** — joins across bronze tables and/or georeferencing (points, distances). Geo tables written as GeoParquet in EPSG:3857.
- **gold** — business aggregations (rankings, per-UF rollups, H3 hexbins).

### Per-app layout
```
apps/<app>/
  metadata.toml              # table registry: locations + per-layer schemas
  jobs/{bronze,silver,gold}/ # one spark-submit script per table
  utils/column_transforms.py # domain-specific column transforms for this app
```

### Shared code (`utils/`)
- `utils/column_transforms.py` — reusable `Column -> Column` functions (text/number/date normalization, null handling, arrays, Sedona geometry helpers). **Check here before writing any generic transform; new generic ones belong here, not in the job body.**
- `utils/dataframe_transforms.py` — reusable `DataFrame -> DataFrame` functions (joins, coordinate validation, H3, geometry reprojection, ranking/window helpers). Chain them with `df.transform(lambda d: dt.some_fn(d, ...))`.
- `utils/metadata.py` — `load_metadata(app)` and `table_location(app, table, level)`.

Import aliases are fixed: `from pyspark.sql import types as T, functions as F`; conventionally `from utils import column_transforms as ct, dataframe_transforms as dt`.

### metadata.toml — the table registry
Each app's `metadata.toml` declares every table: its `name`, available `levels`, natural `key`, and the **full schema per level** (`[[tables.<t>.<level>.schema]]` with `name`/`type`/`description`). `table_location(app, table, level)` reads it and returns `s3a://{bucket}/{app}/{table_name}/{level}` — the bucket is `data_lake.db` overridable by the `S3_BUCKET` env var. It raises `KeyError` for an undeclared table and `ValueError` for a level that isn't ingested yet.

New tables/layers must be declared in `metadata.toml` first, including the per-layer schema. Per the standards, jobs should resolve paths via `table_location(...)` rather than hand-building `s3a://` strings — note that some existing jobs still hardcode paths; prefer the metadata helper in new/edited code.

### Sedona (geospatial)
Geo jobs create the session with `SedonaContext.create(SedonaContext.builder()...getOrCreate())` instead of a plain `SparkSession` — the ST functions are only registered on a Sedona context. Use the Sedona **Python** API (`st_constructors`, `st_functions`), not SQL strings. Distances in meters use geodesic `ST_DistanceSpheroid` on the **original WGS84** coordinates *before* projecting; EPSG:3857 is an output convention only. Geo output is written with `.format("geoparquet")` / `.format("geoparquet").load(...)`.

## Running jobs

The container is a long-running service (`docker-compose.yml`; entrypoint keeps it alive with `tail -f /dev/null`). Jobs are executed inside it via `spark-submit`:

```bash
docker compose up -d --build                 # build image + start container
docker compose exec spark \
  spark-submit apps/<app>/jobs/<layer>/<name>.py
```

- `PYTHONPATH` and `SPARK_CONF_DIR` point at the mounted project root (`/workspace/spark-project`), so `from utils... import` and relative `apps/...` paths work.
- S3A wiring (endpoint, path-style, credentials provider) lives in `conf/spark-defaults.conf`; credentials come from `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` env vars. Copy `.env.example` to `.env` and adjust. The container joins an external Docker network `external-net` to reach the object store (`s3:9000`).
- Spark UI: host port `14040` → `4040`. JupyterLab: `18888` → `8888`.
- Jobs print a `>> <layer> <table>: N registros em <path>` line after writing; they overwrite (`mode("overwrite")`) their output.

There is currently no automated test suite, `validate_setup.py`, or `exploration/` directory in the repo despite references in the container startup banner. Verify changes by running the affected job and checking the output row count / a `.show()`, per the standards' final section.

## Docker build notes

The image builds GDAL 3.8.1 / PROJ 9.3.1 from source (~10 min) in an isolated early layer so it stays cached across dependency changes; Python deps and the Spark/Sedona/S3A jars are downloaded in a later layer. Editing `pyproject.toml` re-runs only the deps layer, not the GDAL compile. Sedona 1.9.0 requires Java 11+ (hence the Fedora 39 base), and PySpark 3.5.x requires Python 3.11 (not Fedora's default 3.12).
