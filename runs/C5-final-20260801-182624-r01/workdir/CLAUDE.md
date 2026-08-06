# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PySpark + Apache Sedona + GDAL geospatial data pipelines over a medallion data
lake, for Brazilian public-data sources. Sources live under `apps/` (`ana`,
`anac`, `bcb`, `caged`, `cvm`, `datasus`, `dnit`, `educacao`, `ibge`, `inmet`,
`snis`, `tse`). This is a **validation environment**: the `apps/*/jobs/` files
are empty scaffolds — the work is implementing the pipeline jobs.

## Read first

- **`CODING_STANDARDS.md`** is binding for all PySpark code. It is a Palantir-style
  guide adapted to this repo. Non-obvious rules that will trip you up:
  - **Language split: code / identifiers / column names in English; docstrings and
    comments in Portuguese.**
  - Transformed column names are `snake_case`, lowercased in the bronze `select`
    via `.alias()`; raw reads keep the source's original casing.
  - No UDFs. No `F.expr` when a native `pyspark.sql.functions` equivalent exists.
  - Model each step as a pure `DataFrame -> DataFrame` (or `Column -> Column`)
    function and chain with `.transform(...)`. **Check `utils/` for an existing
    transform before writing a new generic one**; new generic transforms belong in
    `utils/`, not in the job body.
  - Missing data is `F.lit(None)`, never `""`/`"NA"`/`0`. Absence is not zero.
  - Window functions always get an explicit frame (`rowsBetween`/`rangeBetween`).
  - Geospatial distances in meters use geodesic `ST_DistanceSpheroid` over the
    **original WGS84 coordinates before projecting** — never round-trip through a
    metric CRS just to measure.

## Architecture

**Medallion layers (`levels`):** `raw` → `bronze` → `silver` → `gold`. Not every
table has every layer; the layers a table exposes are declared in metadata.

**`apps/<app>/`** — one directory per data source:
- `metadata.toml` — the source of truth. Declares each table's `name`, `key`, the
  `levels` it exposes, and the **schema per level** (column name, type,
  description). Jobs never hand-build paths or infer schema — they read both from
  here.
- `jobs/` — `spark-submit` entrypoints (one per pipeline job). Currently empty.
- `utils/column_transforms.py` — column transforms specific to that domain
  (e.g. `ibge` has IBGE municipality check-digit validation).

**`utils/`** — shared, generic, testable-in-isolation transforms:
- `column_transforms.py` — `Column -> Column` functions (null handling, BR number/
  date/money parsing, text normalization, arrays/maps/structs, Sedona geometry
  helpers, codes/keys, windows).
- `dataframe_transforms.py` — `DataFrame -> DataFrame` functions (union with schema
  alignment, dedup, coordinate filtering, H3 gridding, reprojection, window
  analytics, no-fanout joins, JSON/nested flattening).
- `metadata.py` — `load_metadata(app)` and `table_location(app, table, level)`,
  which resolves the S3A path `s3a://{bucket}/{app}/{table_name}/{level}`.
  `table_location` **fails fast**: `KeyError` if the table isn't declared,
  `ValueError` if the requested level hasn't been ingested.

**Data lake:** S3A against an S3-compatible object store (MinIO). Bucket comes from
`S3_BUCKET` (falls back to `metadata.data_lake.db`). S3A connection config lives in
`conf/spark-defaults.conf` (loaded via `SPARK_CONF_DIR`), credentials from
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`. Every job ends by **persisting** its
result DataFrame to the layer matching the transformation.

**Imports convention:** `from pyspark.sql import types as T, functions as F`. Sedona
spatial functions come from `sedona.spark.sql` (`st_constructors as stc`,
`st_functions as stf`, plus predicates/aggregates). Sedona helpers require an active
`SedonaContext`.

## Running

Everything runs inside the Docker container (Fedora 39, Java 11, GDAL 3.8.1 + PROJ,
Python 3.11 venv, PySpark 3.5.4, Sedona 1.9.0). The project is mounted at
`/workspace/spark-project`; `PYTHONPATH` is set there so `from utils... import ...`
resolves. The container stays alive (`tail -f`) and jobs are run into it.

```bash
# prerequisite: the compose file expects an external docker network
docker network create external-net

docker compose build           # ~10-min GDAL compile layer, cached
docker compose up -d

# run a job / validation (relative paths work; WORKDIR is the project root)
docker compose exec spark spark-submit apps/<app>/jobs/<job>.py
docker compose exec spark spark-submit validate_setup.py   # end-to-end smoke test
```

Copy `.env.example` to `.env` first. Spark UI is published on `SPARK_UI_PORT`
(default 14040 → container 4040); JupyterLab on `JUPYTER_PORT` (18888 → 8888).

Note: `validate_setup.py` and `exploration/` are referenced by the container
banner (`docker/resources/start.sh`) but do not exist yet — creating them is part
of the validation work. There is no configured test runner yet; per the standards,
verify jobs by running them and confirming the output data.
