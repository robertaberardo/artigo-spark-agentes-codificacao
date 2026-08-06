# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PySpark + Apache Sedona geospatial ETL pipelines over Brazilian public-data sources
(ANA, ANAC, BCB, CAGED, CVM, DATASUS, DNIT, IBGE, INMET, SNIS, TSE). Data lands in an
S3-compatible object store (MinIO) and is processed through a **medallion architecture**:
`raw → bronze → silver → gold`. Runs inside a Fedora/Java 11 Docker container; there is
no host-side Python environment.

## Language convention (important)

Code, identifiers, and column names are **English**; docstrings and comments are
**Portuguese**. Match this when editing — do not translate existing docstrings, and write
new ones in Portuguese. See `CODING_STANDARDS.md` for the full PySpark style guide (adapted
from the Palantir PySpark Style Guide) — read it before writing any transform.

## Architecture

### Per-app layout

Every source is an app under `apps/<app>/`, and each app is self-similar:

```
apps/<app>/
  metadata.toml            # table registry: locations + per-layer schemas (source of truth)
  jobs/{bronze,silver,gold}/<app>_<table>.py   # one spark-submit entrypoint per file
  utils/column_transforms.py                   # domain-specific column helpers for this app
```

`apps/educacao/` is a scaffold: `metadata.toml` + empty `jobs/` packages, no jobs yet —
use it as the template shape when adding jobs.

### The medallion layers

- **bronze** — reads `raw` CSVs (explicit schema, all string columns, `sep=";"`), applies
  column-level cleaning via `utils/column_transforms`, casts to typed columns, writes Parquet.
- **silver** — joins/enriches bronze tables, adds geometry, windowed features (moving avgs,
  ranks), filters to valid rows. Geometry is stored in **EPSG:3857**.
- **gold** — aggregations, rankings, and H3 hexbin grids ready for consumption.

### Metadata drives paths — do not hardcode

`utils/metadata.py::table_location(app, table, level)` resolves every table's S3A path from
the app's `metadata.toml` (`s3a://{S3_BUCKET}/{app}/{name}/{level}`). It raises if the table
isn't declared or the layer hasn't been ingested. **New tables and layers must be declared in
`metadata.toml` (including the per-layer schema) before a job references them.** Jobs should
call `table_location(...)` rather than building `s3a://...` strings by hand — a handful of
older gold jobs still hardcode paths; prefer the metadata pattern for new/edited code.

### Shared vs app-specific transforms

- `utils/column_transforms.py` — generic `Column -> Column` helpers (null handling, BR
  number/date parsing, text normalization, Sedona geometry ops, H3, keys).
- `utils/dataframe_transforms.py` — generic `DataFrame -> DataFrame` helpers (joins without
  fan-out, unions across schemas, window features, H3 aggregation, reprojection, flattening).
- `apps/<app>/utils/column_transforms.py` — helpers specific to that domain
  (e.g. `is_valid_station_code` for ANA).

**Before writing a new generic transform, check `utils/` — reuse it.** New generic transforms
belong in `utils/`, not inline in a job. Jobs compose these via `DataFrame.transform(...)`
chains rather than reassigning intermediate variables.

### Job structure (the repeating pattern)

Each job file defines module-level path/schema constants and a `main()` that: builds a
`SparkSession`, reads the input, composes transforms, `write.mode("overwrite").parquet(DEST)`,
then re-reads to `print` a row count. Sedona spatial functions require an active
`SedonaContext` — helpers whose docstrings say *"Requer SedonaContext ativa"* will fail
without it.

## Running things

Everything runs in the container defined by `docker-compose.yml` / `Dockerfile`. The project
is mounted at `/workspace/spark-project`; `PYTHONPATH` is the project root (so
`from utils.column_transforms import ...` works). Requires an external Docker network named
`external-net` and a reachable S3 service (the MinIO/S3 container is expected at `s3:9000`).

```bash
docker compose build                       # build the image (GDAL compile is ~10 min, cached)
docker compose up -d                       # start container (stays alive via `tail -f`)
docker compose exec spark bash             # shell into it

# Run a single job (from PROJECT_PATH, relative paths work):
docker compose exec spark spark-submit apps/ana/jobs/bronze/ana_estacao.py
```

Because layers depend on earlier layers, run a table's jobs in order (bronze → silver → gold).

- Config: copy `.env.example` → `.env` (S3 endpoint/credentials, UI ports).
- Spark UI: host `:14040` (`SPARK_UI_PORT`). Jupyter: host `:18888` (`JUPYTER_PORT`).
- S3A wiring (endpoint, path-style, env-var credentials) lives in `conf/spark-defaults.conf`,
  read via `SPARK_CONF_DIR` — keep connection config out of job code.

Note: `start.sh` advertises `validate_setup.py` and an `exploration/` Jupyter launcher that
are not present in the repo yet.

## Testing

There is no automated test suite. Verify a change by running its job in the container and
checking the printed row count / inspecting the written Parquet, per `CODING_STANDARDS.md`.

## Non-negotiable conventions (from CODING_STANDARDS.md)

- **No UDFs** — express everything with native `pyspark.sql.functions`. Prefer
  `F.some_func(...)` over `F.expr("...")`.
- **Absence is `F.lit(None)`, never `""`/`"NA"`/`0`** — and don't zero-fill nulls before
  aggregating (`mean`/`sum` already skip nulls).
- **`select` is the schema contract** — cast inside `select`, not with `withColumn`; at most
  one function per column there.
- **Coordinates: lat/long always `double`.** Measure geodesic distance
  (`ST_DistanceSpheroid`) on the **original WGS84** coords *before* projecting to 3857 — never
  round-trip through a projected CRS just to measure.
- **Joins**: always state `how`; avoid `right` joins; never paper over duplicate keys with
  `.dropDuplicates()`/`.distinct()` (see `join_no_fanout`).
- **Window functions**: always give an explicit frame (`rowsBetween`/`rangeBetween`); avoid
  empty `partitionBy()`.
- Import aliases: `from pyspark.sql import types as T, functions as F`. Files ≲250 lines,
  functions ≲70 lines. No commented-out code.
