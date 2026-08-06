# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PySpark + Apache Sedona (geospatial) + GDAL batch pipelines that ingest Brazilian
public-sector open data into a medallion-architecture data lake stored on
S3-compatible object storage (MinIO/S3, via Spark's S3A connector). Python 3.11,
PySpark 3.5.4, Sedona 1.9.0.

`CODING_STANDARDS.md` is the authoritative PySpark style guide (Portuguese, based on
Palantir's) — read it before writing or reviewing job code. The rules below are the
architecture; the standards file is the coding conventions. Both matter.

## Running jobs

Everything runs inside the Docker container — the host has no Spark/GDAL. The image
compiles GDAL from source (~10 min first build); layers are ordered so editing
`pyproject.toml` does not re-trigger the GDAL build.

```bash
docker compose up -d --build          # build image, start long-lived container
                                       # (entrypoint is `tail -f /dev/null`)
docker compose exec spark spark-submit apps/<app>/jobs/<layer>/<job>.py
```

- Requires an external Docker network named `external-net` and a reachable S3 endpoint
  (`docker network create external-net`; the object store runs separately).
- Config comes from env (`.env`, see `.env.example`): `S3_ENDPOINT`, `S3_BUCKET`,
  `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`. `S3_BUCKET` overrides the metadata `db`
  when building table paths.
- S3A wiring lives in `conf/spark-defaults.conf` (loaded via `SPARK_CONF_DIR`), not in
  job code. Credentials are read from the AWS env vars.
- `PYTHONPATH=/workspace/spark-project` (repo root) so jobs can
  `from utils... import ...` and `from apps.<app>.utils... import ...`.
- There is no test runner or linter configured. "Testing" a job means running it and
  checking the row count / `.show()` it prints, or validating output in Jupyter
  (`[dev]` extra installs JupyterLab; Spark UI on 4040, Jupyter on 8888 — host ports
  `SPARK_UI_PORT`/`JUPYTER_PORT`).

## Architecture

### Medallion layers (raw → bronze → silver → gold)

Each job reads one layer and writes the next; jobs are independent scripts with a
`main()` and are chained by running them in dependency order (there is no orchestrator
in-repo).

- **raw** — source files exactly as landed (CSV, `;`-separated, original column
  casing). Read with an **explicit schema** (never `inferSchema`), all columns typed
  `string`.
- **bronze** — one job per source table. Reads raw, standardizes column names to
  `snake_case` via `.alias()` in a single `select`, applies cleaning column-transforms,
  casts to real types (lat/long → `double`, quantities → `int`), filters invalid keys.
- **silver** — joins/consolidates bronze tables and/or derives geometry. Business
  entities resolved and enriched.
- **gold** — aggregates/rankings for consumption (per-UF rollups, route rankings, H3
  hexbin density).

Output is Parquet, `mode("overwrite")`. Geospatial silver/gold tables are written as
**GeoParquet** (`.format("geoparquet")`) instead.

### Apps

`apps/<app>/` is one data source (`anac`, `bcb`, `caged`, `cvm`, `datasus`, `dnit`,
`educacao`, `ibge`, `inmet`, `snis`, `tse`, `ana`). Each contains:

- `jobs/{bronze,silver,gold}/<table>.py` — the pipeline steps.
- `utils/column_transforms.py` — column-transforms specific to that domain (e.g. ANAC's
  `is_valid_oaci`). Domain-specific logic lives here, not in the job body.
- `metadata.toml` — the table catalog (see below).

### Table metadata (`apps/<app>/metadata.toml`)

The **single source of truth** for where each table lives and its schema per layer.
Jobs must not hand-build S3 paths — call `utils.metadata.table_location(app, table,
level)`, which returns `s3a://{S3_BUCKET or db}/{app}/{table_name}/{level}` and fails
fast (`KeyError`/`ValueError`) if the table or layer is not declared. New tables and
layers must be declared here first, with per-layer schema (`[[tables.<t>.<level>.schema]]`
entries: `name`/`type`/`description`).

> Note: not all existing jobs use `table_location` yet — some hardcode the `s3a://...`
> path as module constants. `table_location` (metadata-driven) is the intended pattern;
> prefer it for new code.

### Shared transforms (`utils/`)

Reusable, source-agnostic building blocks. **Check here before writing any generic
transform** — new generic transforms belong here, not inline in a job.

- `utils/column_transforms.py` — pure `Column -> Column` functions (text normalization,
  BR number/date/money parsing, null-safety, arrays/maps/structs, Sedona geometry
  validity, keys). Compose via `.select(...)`.
- `utils/dataframe_transforms.py` — `DataFrame -> DataFrame` functions (joins without
  fan-out, unions, windows/ranking, H3 gridding, reprojection, distance, flattening).
  Compose via `df.transform(fn)`.
- `utils/metadata.py` — metadata loading and `table_location`.

### Geospatial (Sedona)

- A job that uses geometry must create the session via
  `SedonaContext.create(SedonaContext.builder()...getOrCreate())`, not a plain
  `SparkSession` — the ST functions are only registered on a Sedona context.
- Point order is `ST_Point(x=longitude, y=latitude)`.
- Distances use **geodesic** `ST_DistanceSpheroid` on WGS84 (EPSG:4326) — measure on the
  original lat/long *before* reprojecting; never round-trip through a metric CRS to
  measure. Geometry is stored reprojected to EPSG:3857 (Web Mercator) as an output
  convention only.
- H3 gridding operates in EPSG:4326; helpers reproject internally when the input CRS
  differs.

## Conventions that bite (from CODING_STANDARDS.md)

- Code/identifiers/column names in **English**; docstrings and comments in **Portuguese**.
- Model each step as a pure `DataFrame -> DataFrame` / `Column -> Column` function;
  chain with `.transform(...)`, not reassignment. **No UDFs** — use native
  `pyspark.sql.functions`. Prefer `F.func(...)` over `F.expr("...")`.
- Absence is `F.lit(None)`, never `""`/`"NA"`/`0`. Don't zero-fill nulls before
  aggregating (`avg`/`sum` already skip nulls).
- Every `select` is a schema contract: at most one `F.` function + optional `.alias()`
  per column; cast inside the `select`, not with a later `withColumn`.
- Joins: always state `how` explicitly; avoid `right` joins; do **not** paper over
  unexpected duplicates with `dropDuplicates()`/`distinct()` (there is a cause —
  `dataframe_transforms.join_no_fanout` turns silent fan-out into an error).
- Window functions: always give an explicit frame (`rowsBetween`/`rangeBetween`); avoid
  empty `partitionBy()` — aggregate instead.
- `from pyspark.sql import types as T, functions as F`. Files ≲250 lines, functions ≲70,
  chains ≲5 expressions. Comment the *why*, never leave commented-out code.
