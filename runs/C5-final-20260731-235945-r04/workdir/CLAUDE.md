# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PySpark data pipelines that ingest Brazilian public-sector open data (ANAC, DATASUS,
IBGE, TSE, BCB, DNIT, INMET, ANA, CAGED, CVM, SNIS, educação) into an S3-compatible
data lake (MinIO), following a medallion architecture: **raw → bronze → silver → gold**.
Geospatial work uses Apache Sedona + GDAL. Runs are validated locally in Docker before
being promoted to the production cluster.

**`CODING_STANDARDS.md` is the authoritative PySpark style guide — read it before writing
or reviewing any job or transform.** Key rules that are easy to get wrong are summarized
below, but that file governs.

## Language convention (non-obvious, enforced)

- **Code, function/variable names, and column names: English.**
- **Docstrings and comments: Portuguese.**
- Transformed columns (bronze onward) are `snake_case` lowercase. Raw reads reference the
  source's **original** column names; lowercasing happens in the bronze `select` via `.alias()`.

## Architecture

### Per-app layout
Each source is an app under `apps/<app>/`:
```
apps/<app>/
  metadata.toml              # declares every table: levels, key, and per-level schema
  jobs/{bronze,silver,gold}/ # one spark-submit script per output table
  utils/column_transforms.py # app-specific Column->Column helpers (e.g. is_valid_oaci)
```

### metadata.toml is the schema/location source of truth
`metadata.toml` declares each table's `name`, the `levels` it exists at, its `key`, and a
schema (name/type/description) **per level**. Jobs must not hand-build lake paths — resolve
them via `utils.metadata.table_location(app, table, level)`, which returns
`s3a://{S3_BUCKET or db}/{app}/{name}/{level}` and raises early if the table/level isn't declared.
New tables or layers must be added to `metadata.toml` first, including the per-layer schema.

> Note: bronze jobs consistently use `table_location`, but many silver/gold jobs currently
> hardcode `s3a://...` path constants. When touching those, prefer migrating to `table_location`.

### Job anatomy
Every job is a standalone `spark-submit` script exposing `main()`:
- **Bronze**: reads raw (CSV with explicit `StructType` schema — never `inferSchema`),
  standardizes via shared/app transforms, filters invalid keys, writes bronze parquet.
- **Silver**: joins/enriches bronze tables; geospatial silver writes `geoparquet`.
- **Gold**: aggregates to analytics tables (rankings, per-UF rollups, H3 hexbins).
- Each ends by **persisting** the result, then reads it back to `print` a row count.

### Shared transforms (reuse before writing new)
- `utils/column_transforms.py` — `Column -> Column` (e.g. `normalize_text`, `standardize_uf`,
  `to_coordinate`, `safe_divide`, `blank_to_null`, array/map helpers).
- `utils/dataframe_transforms.py` — `DataFrame -> DataFrame` (e.g. `join_lookup`,
  `filter_valid_coordinates`, `add_point_geometry`, `to_web_mercator`, `attach_h3_index`,
  `rank_within_group`).

Model each step as a pure function and chain with `df.transform(...)`. **Before writing a
generic transform, check `utils/` and reuse it.** New generic transforms belong in `utils/`,
not inline in a job. No UDFs.

### Geospatial (Sedona)
Geospatial jobs create the session with `SedonaContext.create(SedonaContext.builder()...)`
instead of a plain `SparkSession`. Latitude/longitude are always `double`. Measure distances
with geodesic functions (`ST_DistanceSpheroid`, WGS84) **on the original coordinates before
projecting**; geometry is written in EPSG:3857 as an output convention only. See the geospatial
section of `CODING_STANDARDS.md`.

## Running jobs

Everything runs inside the Docker image (`spark-pipelines:local`), which bundles Java 11,
Python 3.11 venv, GDAL 3.8.1, and pre-downloaded Sedona/S3A jars.

```bash
docker compose up -d --build          # build + start the long-lived container
# run a job (paths are relative to the mounted project root):
docker compose exec spark spark-submit apps/<app>/jobs/<layer>/<job>.py
```

Config comes from env / `.env` (copy from `.env.example`): S3 credentials
(`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` via the S3A `EnvironmentVariableCredentialsProvider`),
`S3_ENDPOINT`, `S3_BUCKET`. Spark UI → host `14040`, Jupyter → `18888`.

**Prerequisites:** compose attaches to an **external** Docker network `external-net` and expects
an S3-compatible store reachable at `s3:9000` on it — both must exist beforehand (this repo does
not define the MinIO service). S3A/endpoint plumbing lives in `conf/spark-defaults.conf` (loaded
via `SPARK_CONF_DIR`), keeping connection config out of job code.

## Tests / lint

There is no test suite, linter, or CI config committed in this repo. `CODING_STANDARDS.md`
asks that changes be verified by running the relevant job locally (the count/`show` printed by
`main()`) or checking output data manually.
