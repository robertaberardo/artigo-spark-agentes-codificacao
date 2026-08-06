# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PySpark 3.5.4 + Apache Sedona 1.9.0 + GDAL geospatial data pipelines over Brazilian public-data sources, run as a "validation environment" that mirrors a production Spark cluster. Everything runs inside a Docker container (Fedora 39, Java 11, Python 3.11 pinned); the data lake is an S3-compatible object store (MinIO) reached over the S3A connector.

## Architecture

**Medallion lake, one folder tree per source.** Each of the 12 apps under `apps/` (`ana`, `anac`, `bcb`, `caged`, `cvm`, `datasus`, `dnit`, `educacao`, `ibge`, `inmet`, `snis`, `tse`) is a self-contained pipeline with the same shape:

```
apps/<app>/
  metadata.toml              # source of truth: tables, layers, per-layer schemas, keys
  jobs/bronze/<table>.py     # raw CSV -> cleaned parquet
  jobs/silver/<table>.py     # bronze -> geo-referenced / consolidated (geoparquet)
  jobs/gold/<table>.py       # silver -> aggregations, rankings, H3 hexbins
  utils/column_transforms.py # transforms specific to THIS domain
```

Data flows `raw → bronze → silver → gold`. One job file = one output table. Each job reads its input layer, transforms, and **always persists** the result to the next layer before exiting.

**`metadata.toml` is the schema contract — jobs never hard-code paths.** Table locations come from `utils.metadata.table_location(app, table, level)`, which builds `s3a://{bucket}/{app}/{table_name}/{level}` and raises if the table isn't declared or the layer hasn't been ingested (fail-early, not empty-prefix reads). Any new table or layer must be declared in the app's `metadata.toml`, including its per-layer schema. Schemas there are explicit and typed (note: `latitude`/`longitude` are `string` in raw, `double` from bronze on).

**Shared vs. domain transforms.** Generic, reusable transforms live in top-level `utils/` and are the first place to look before writing anything new:
- `utils/column_transforms.py` — `Column -> Column` functions (parsing BR numbers/dates/money, null hygiene, WKT/geometry helpers, keys, bucketize, …).
- `utils/dataframe_transforms.py` — `DataFrame -> DataFrame` functions (geometry construction, reprojection, H3 gridding/aggregation, windowed ranking/moving-average, fan-out-safe joins, unions, flattening).
- `utils/metadata.py` — metadata/path resolution.

Domain-specific transforms (e.g. `apps/ana/utils/column_transforms.py`'s `is_valid_station_code`) live under the app, never in the job body. New *generic* transforms belong in `utils/`, not in a job.

**Bronze vs. spatial jobs differ in the session.** Bronze jobs use a plain `SparkSession` and read header-`;`-CSV with an explicit `StructType` (never `inferSchema`), writing `.parquet`. Silver/gold jobs that touch geometry create a `SedonaContext` (`SedonaContext.create(SedonaContext.builder()...)`) and read/write `geoparquet`. Any Sedona `ST_*` function requires an active `SedonaContext`.

## Commands

Run from the repo root. The container mounts the whole project at `/workspace/spark-project`, sets `PYTHONPATH` there (so `from utils...` and `from apps.<app>.utils...` resolve), and stays alive (`tail -f`) — you exec jobs into the running container rather than the container running them.

```bash
# Build the image (GDAL compile is ~10 min; cached across dependency changes)
docker compose build

# Start the container (requires the external `external-net` network and an `s3` service)
docker compose up -d

# Run one job (paths are relative to the mounted project root)
docker compose exec spark spark-submit apps/ana/jobs/bronze/ana_estacao.py

# Run a whole app in medallion order: bronze, then silver, then gold
docker compose exec spark spark-submit apps/ana/jobs/silver/ana_estacao_geo.py
docker compose exec spark spark-submit apps/ana/jobs/gold/ana_estacoes_hexbin.py

# Spark UI: http://localhost:14040   Jupyter: http://localhost:18888
```

There is no automated test suite; jobs are validated by running them and checking the row counts/output each job prints (see `CODING_STANDARDS.md` final note). Environment defaults (S3 endpoint/bucket/credentials, ports) come from `.env` — copy `.env.example`.

## Conventions (see CODING_STANDARDS.md — it is authoritative)

`CODING_STANDARDS.md` is a Palantir-style PySpark guide in Portuguese and governs all pipeline code. The load-bearing rules:

- **Language split:** code, function/variable/column names in **English**; docstrings and comments in **Portuguese**. Comment the *why*, not the *what*.
- **Column names** are lowercase `snake_case` from bronze onward; the raw read keeps the source's original casing, standardized to lowercase via `.alias(...)` in the bronze `select`.
- **Model every step as a pure `DataFrame -> DataFrame` (or `Column -> Column`) function** and chain with `.transform(...)`. Check `utils/` for an existing one before writing a new transform.
- **No UDFs; no `F.expr` when a native `pyspark.sql.functions` equivalent exists.** Prefer `F.col("x")` references and native functions.
- **`select` is the schema contract** — cast inside `select`, at most one function per column; extract to `clean_<name>()` past three.
- **Absence is never zero:** fill empties with `F.lit(None)`, never `""`/`"NA"`; don't turn nulls into `0` before aggregating.
- **Joins:** always state `how`; avoid `right` joins; watch for fan-out; never mask duplicates with `dropDuplicates`/`distinct`. `dataframe_transforms.join_no_fanout` enforces this.
- **Window functions always specify an explicit frame** (`rowsBetween`/`rangeBetween`); avoid empty `partitionBy()`.
- **Geospatial:** use Sedona's Python API over hand-built SQL. Measure metric distances with **geodesic** `ST_DistanceSpheroid` over the **original WGS84** coordinates *before* projecting; do not round-trip `4326→3857→4326` just to measure. Output geometry is persisted in EPSG:3857 (a convention, separate from how distance is computed).
- Keep files ≲250 lines, functions ≲70; import as `from pyspark.sql import types as T, functions as F`; leave no commented-out code.
