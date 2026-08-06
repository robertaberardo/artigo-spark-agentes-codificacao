# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PySpark + Apache Sedona + GDAL pipelines that ingest Brazilian public-data sources into a
medallion-architecture data lake. This is a **validation/dev environment**: the data lake lives
on an S3-compatible object store (MinIO) reached over S3A, and jobs run inside a Docker container.

There are 12 source apps under `apps/`, one per data provider:
`ana` (water resources), `anac` (aviation), `bcb` (central bank), `caged` (employment),
`cvm` (securities/funds), `datasus` (health), `dnit` (roads), `educacao`, `ibge`, `inmet`
(meteorology), `snis` (sanitation), `tse` (elections). Each is self-contained and follows the
same layout.

## Architecture

**Medallion layers → directories.** Every app has `jobs/{bronze,silver,gold}/`:
- **bronze** — reads the raw source (CSV: `header=True`, `sep=";"`, **explicit schema, never
  `inferSchema`**), standardizes column names to lowercase `snake_case` via `.alias(...)`, casts
  types, applies column-cleaning utils, and writes Parquet. Raw stays as it lands; the
  lowercasing happens in the bronze `select`.
- **silver** — joins/enriches/geo-references. Geospatial tables are written as `geoparquet`
  with geometry in **EPSG:3857**.
- **gold** — aggregations, rankings, H3 hexbins for consumption.

**A job is a standalone `spark-submit` script.** Each `.py` under `jobs/` defines `main()` that
builds a `SparkSession` (or `SedonaContext.create(...)` for geo jobs), reads source(s),
transforms, writes the result, prints a `>> <layer> <table>: N registros em <path>` line, and
calls `spark.stop()`. There is no orchestrator, DAG, or shared driver — jobs are run one at a
time and depend on prior layers already being materialized.

**Table paths come from metadata, not string literals.** `apps/<app>/metadata.toml` declares
every table, its `levels`, its `key`, and the **per-layer schema**. Jobs resolve locations with
`table_location(app, table, level)` from `utils/metadata.py`, which returns
`s3a://{bucket}/{app}/{table_name}/{level}` (bucket overridable via `S3_BUCKET`). It raises if
the table or level isn't declared — fail early instead of reading an empty prefix. When adding a
table or layer, declare it in `metadata.toml` first.
*(Note: a few older jobs, e.g. `apps/ana/jobs/gold/ana_ranking_estacoes.py`, still hardcode
`s3a://...` paths. New code should use `table_location`.)*

**Transformations are pure functions, composed with `.transform()`.** Shared, reusable logic
lives in `utils/`, split by return type:
- `utils/column_transforms.py` — `Column -> Column` (text/coordinate/array/geometry helpers).
- `utils/dataframe_transforms.py` — `DataFrame -> DataFrame` (geo ops, H3 indexing, windowed
  ranks/moving averages, joins-without-fanout, etc.).
- `apps/<app>/utils/column_transforms.py` — **domain-specific** column helpers for that source
  only (e.g. `is_valid_station_code`).

**Before writing a new generic transform, check `utils/` for an existing one** and reuse it.
Genuinely generic new transforms belong in `utils/`, not in the job body.

## Running jobs

Everything runs in the `spark-pipelines` container (Fedora 39, Java 11, Python 3.11 venv at
`/opt/venv`, GDAL 3.8.1, Spark 3.5.4 + Sedona 1.9.0). `PYTHONPATH=/workspace/spark-project`
makes `from utils import ...` and `from apps.<app>... import ...` resolve.

```bash
# Requires an external Docker network named `external-net` and an S3 service
# reachable at s3:9000 (see docker-compose.yml / .env). Copy .env.example -> .env first.
docker network create external-net          # once, if it doesn't exist
docker compose up --build -d                 # build image, start container (stays alive)

# Run a single job (from the project root inside the container):
docker compose exec spark spark-submit apps/ana/jobs/bronze/ana_estacao.py
docker compose exec spark spark-submit apps/<app>/jobs/<layer>/<job>.py
```

Spark UI is exposed on `${SPARK_UI_PORT:-14040}` → container 4040; Jupyter on
`${JUPYTER_PORT:-18888}` → 8888.

**No automated test suite ships with the repo.** Per `CODING_STANDARDS.md`, validate by running
the job and confirming the printed row count / output, or by checking transforms manually.
Because each transform is a pure function, it can be exercised in isolation in Jupyter.

## Conventions that will trip you up

Full rules are in **`CODING_STANDARDS.md`** (Portuguese, based on the Palantir PySpark style
guide) — read it before non-trivial changes. The load-bearing ones:

- **Language split:** code, function/variable names, and column names in **English**; docstrings
  and comments in **Portuguese**.
- **No UDFs.** Rewrite with native `pyspark.sql.functions`. Prefer `F.<fn>(...)` over
  `F.expr("...")`; only fall back to `F.expr` when no native equivalent exists, and comment why.
- **Absence is `NULL`, never `""` or `0`.** Use `F.lit(None)`. Don't zero-fill nulls before
  aggregating — `avg`/`sum` already ignore nulls, and zeroing distorts them.
- **`select` is the schema contract.** One `spark.sql.functions` call + optional `.alias()` per
  column; cast inside the `select`, not with `withColumn`. Extract a `clean_<name>()` if it grows.
- **Coordinates:** latitude/longitude are always `double` (never string). Filter to valid WGS84
  before using.
- **Geospatial (Sedona):** use the Python API over hand-built SQL. Compute distances
  **geodesically** (`ST_DistanceSpheroid`, WGS84 ellipsoid) on the **original** coordinates
  **before** projecting to 3857 — never round-trip `4326→3857→4326` just to measure. 3857 is an
  output convention for the persisted geometry only.
- **Windows:** always give an explicit frame (`rowsBetween`/`rangeBetween`); avoid empty
  `partitionBy()`.
- **Joins:** always state `how`; avoid `right` (flip and use `left`); don't paper over
  duplicates with `.distinct()`/`.dropDuplicates()`.
- **Imports:** `from pyspark.sql import types as T, functions as F`.
- Files ≲250 lines, functions ≲70 lines, chains ≲5 expressions.
