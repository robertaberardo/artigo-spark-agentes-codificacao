# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A monorepo of **PySpark + Apache Sedona** batch pipelines that ingest and transform Brazilian public open-data sources (ANA, ANAC, BCB, CAGED, CVM, DATASUS, DNIT, IBGE, INMET, SNIS, TSE; `educacao` is an empty scaffold) into a medallion-architecture data lake on S3A. This is a validation environment — it runs against a MinIO/S3 object store, not the production cluster.

## Language convention (strict)

- **Code, function/variable names, and column names: English.**
- **Docstrings and comments: Portuguese.**

Match this when adding code. `CODING_STANDARDS.md` (Portuguese) is the authoritative PySpark style guide — read it before writing or reviewing any Spark transformation. The rules below are load-bearing and easy to get wrong; the rest of the guide fills in the detail.

## Architecture

### Medallion layers
Data flows `raw → bronze → silver → gold`, one directory of jobs per layer inside each app:

- **bronze** (`jobs/bronze/`): read `raw` CSVs with an **explicit** `StructType` schema (never `inferSchema`), standardize column names to `snake_case` via `.alias()` in a single `select`, apply column-level cleaning from `utils`, write Parquet. This is where source-cased raw names become lowercase.
- **silver** (`jobs/silver/`): join/enrich/consolidate; this is where geospatial work happens (point construction, distance, reprojection). Geo tables are written as **`geoparquet`**.
- **gold** (`jobs/gold/`): aggregations, rankings, and H3 hexbin density tables for downstream consumption.

### Per-app layout
Every app under `apps/<app>/` follows the same shape:
```
apps/<app>/
  metadata.toml              # table registry + per-layer schemas (the contract)
  jobs/{bronze,silver,gold}/ # one .py file per output table
  utils/column_transforms.py # domain-specific Column→Column helpers (e.g. is_valid_station_code)
```

### Shared `utils/` (reuse before writing new)
Before writing any generic transformation, **check `utils/` first** and reuse it. New generic transforms belong in `utils/`, not in a job body.
- `utils/column_transforms.py` — pure `Column → Column` functions (text/number/date parsing, BR decimal/money, coordinate parsing, geometry validation, keys). Brazilian-format parsers live here (`br_decimal_to_double`, `money_br_to_double`, `parse_br_date`).
- `utils/dataframe_transforms.py` — pure `DataFrame → DataFrame` functions (joins, windows/ranking/moving-average, H3 gridding, reprojection, unions). Chain these with `df.transform(...)`.
- `utils/metadata.py` — `load_metadata()` and `table_location()`.

### metadata.toml is the schema contract
Table locations and per-layer schemas are declared in each app's `metadata.toml`; **jobs must not hand-build lake paths**. `table_location(app, table, level)` returns `s3a://{db}/{app}/{name}/{level}`, validating that the table and level are declared (raises `KeyError`/`ValueError` early instead of reading an empty prefix). The `db`/bucket is overridable via the `S3_BUCKET` env var. New tables and layers must be declared in `metadata.toml`, including the schema for each layer.

> Note: canonical jobs derive paths via `table_location(...)` and guard `main()` with `if __name__ == "__main__":`. Some existing jobs drift from this (hardcoded `s3a://...` module constants, or a bare `main()` call). Prefer the canonical form for new code.

## Non-obvious rules that break data if ignored

- **Geospatial distance:** measure geodesic distance (`ST_DistanceSpheroid`, WGS84 ellipsoid) on the **original WGS84 lat/long, BEFORE projecting to EPSG:3857**. Never round-trip `4326→3857→4326` just to measure — it loses precision. Persisting geometry in 3857 is an output convention only; compute distances first, project the stored geometry after. Lat/long are always `double`, never string.
- **Absence is never zero:** fill empty columns with `F.lit(None)`, never `""`/`"NA"`/`0`. Do not coalesce nulls to `0` before aggregating (`avg`/`sum` already ignore nulls) unless the contract explicitly says absence counts as zero.
- **No UDFs, no `F.expr`** when a native `pyspark.sql.functions` (or Sedona Python API) equivalent exists. Native forms are analysis-time checked.
- **Joins:** always pass `how` explicitly; avoid `right` joins (flip and use `left`). Do not use `.dropDuplicates()`/`.distinct()` to paper over unexpected duplicates — investigate the fan-out. `dataframe_transforms.join_no_fanout` enforces this.
- **Window functions:** always specify an explicit frame (`rowsBetween`/`rangeBetween`); avoid empty `partitionBy()` (forces a single partition — aggregate instead).
- **`select` is the schema contract:** at most one `spark.sql.functions` call per column plus an optional `.alias()`; do casts inside `select`, not with `withColumn`. Files ≤ ~250 lines, functions ≤ ~70 lines.
- Established import aliases: `from pyspark.sql import types as T, functions as F`. Sedona: `from sedona.spark.sql import st_constructors as stc, st_functions as stf`.

## Environment and running jobs

Runs inside a Docker container (Fedora 39, Java 11, Python 3.11, GDAL 3.8.1). Spark/Sedona/S3A jars are pre-baked into the image (`docker/resources/setup-env.sh`); they are not resolved via Ivy at runtime.

```bash
# Build + start (needs an external docker network named "external-net")
docker compose up -d --build

# Run a job (WORKDIR is the project root; PYTHONPATH includes it so `from utils...` works)
docker compose exec spark spark-submit apps/<app>/jobs/<layer>/<job>.py

# Interactive shell in the container
docker compose exec spark bash
```

Config lives outside job code:
- S3A connection (endpoint, path-style access, env-var credentials provider) is in `conf/spark-defaults.conf`, read via `SPARK_CONF_DIR`.
- Credentials and ports come from `.env` (see `.env.example`): `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, `S3_ENDPOINT`, `S3_BUCKET`.

There is no test suite or linter configured. Verify changes by running the job and checking the output count/schema (each job prints a `>> <layer> <table>: N registros` line and reads the output back). Geospatial jobs must create their Spark session via `SedonaContext.create(SedonaContext.builder()...)` — the plain `SparkSession` builder does not register `ST_*` functions.
