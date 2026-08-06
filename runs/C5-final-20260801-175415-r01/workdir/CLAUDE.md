# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PySpark 3.5.4 + Apache Sedona 1.9.0 + GDAL geospatial data pipelines over Brazilian
open-data sources (ANA, ANAC, BCB, CAGED, CVM, DATASUS, DNIT, Educação, IBGE, INMET,
SNIS, TSE). Data flows through a **medallion architecture** (`raw → bronze → silver →
gold`) and is persisted to an S3-compatible object store (MinIO) as Parquet/GeoParquet.

## Language convention (non-obvious, enforced)

- **Code, identifiers, and column names: English.**
- **Docstrings and comments: Portuguese.**
- Column names are `snake_case` lowercase from **bronze** onward. Raw reads keep the
  source's original column casing; the lowercase aliasing happens in the bronze `select`.

## Architecture

Each `apps/<app>/` is one data source and is self-contained:

```
apps/<app>/
  metadata.toml              # source of truth: table paths + per-layer schemas
  jobs/{bronze,silver,gold}/ # one spark-submit-able .py per output table
  utils/column_transforms.py # domain-specific Column->Column transforms
```

**Layers** (a job's layer = the nature of its transformation, and dictates its format):
- `raw` — source files (CSV, `;`-separated, original column names).
- `bronze` — typed + cleaned + `snake_case`, one table per source table. Plain
  `SparkSession`, written as **Parquet**.
- `silver` — geo-referenced / joined / consolidated. Geo tables use `SedonaContext`
  and are written as **GeoParquet**; geometry persisted in **EPSG:3857**.
- `gold` — aggregations, rankings, H3 hexbin densities.

### metadata.toml is the only source of table paths

Jobs never build paths by hand. They resolve every table through
`utils.metadata.table_location(app, table, level)`, which returns
`s3a://{S3_BUCKET or db}/{app}/{table_name}/{level}` and **fails early** (`KeyError` if
the table isn't declared, `ValueError` if that layer hasn't been ingested). New tables
and layers — including their per-layer schema — must be declared in `metadata.toml`
first.

### Shared vs. domain transforms

- `utils/column_transforms.py` — generic `Column -> Column` (text/number/date/coord/geo helpers).
- `utils/dataframe_transforms.py` — generic `DataFrame -> DataFrame` (joins, windows, H3, reprojection, geometry).
- `apps/<app>/utils/column_transforms.py` — transforms specific to that source's domain.

**Before writing any generic transform, check `utils/` for an existing one and reuse it.**
New *generic* transforms belong in `utils/`, never inline in a job.

### Job shape

Read the schema explicitly (never `inferSchema`), chain pure `.transform(fn)` steps
(functions modelled as `DataFrame -> DataFrame` / `Column -> Column`), then
`write.mode("overwrite")` and print a row count. Geo jobs create the session via
`SedonaContext.create(SedonaContext.builder()...)` and read/write `format("geoparquet")`.

## Geospatial rules (easy to get wrong)

- Measure distances with **geodesic** `ST_DistanceSpheroid` over the **original WGS84**
  coordinates, **before** projecting to 3857. Never round-trip `4326→3857→4326` to
  measure — it loses precision. Persisting geometry in 3857 is an output convention, not
  a measurement step.
- Latitude/longitude are always `double`, never string.
- Prefer the Sedona **Python API** (`st_functions`, `st_constructors`) over hand-written SQL.

## Commands

Everything runs inside the `spark` container (Fedora + Java 11 + Python 3.11 venv +
pre-downloaded Sedona/S3A jars). Requires an external Docker network `external-net` and a
reachable MinIO/S3 at `S3_ENDPOINT`.

```bash
docker compose build                 # build image (GDAL compile is ~10 min, cached)
docker compose up -d                 # start container (stays alive via tail -f)

# Run one job (paths are relative to the project root = WORKDIR):
docker compose exec spark spark-submit apps/<app>/jobs/<layer>/<job>.py

# Spark UI: http://localhost:14040   Jupyter: http://localhost:18888
```

Config: copy `.env.example` to `.env`. S3A wiring (endpoint, path-style, credentials
provider) lives in `conf/spark-defaults.conf`, loaded via `SPARK_CONF_DIR`; credentials
come from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`. `PYTHONPATH` is the project root
so jobs can `from utils... import` and `from apps.<app>... import`.

There is no test runner or lint config in the repo; validate a job by running it and
checking the printed row count / output table.

## Coding standards

`CODING_STANDARDS.md` (Portuguese, adapted from the Palantir PySpark Style Guide) is
authoritative for all PySpark. Load-bearing points:

- Model each step as a pure function chained with `DataFrame.transform(...)`; **no UDFs**
  (use native `pyspark.sql.functions`), and prefer native `F.<fn>(...)` over `F.expr("...")`.
- `select` is the schema contract: cast inside `select`, ≤1 function per column, reference
  columns as `F.col("name")`, rename via `.alias()` (not chained `withColumnRenamed`).
- **Absence is never zero:** fill empties with `F.lit(None)`, never `""`/`"NA"`; don't
  zero-fill nulls before aggregating.
- Always specify an explicit window frame (`rowsBetween`/`rangeBetween`); avoid empty
  `partitionBy()` — aggregate instead. Always specify join `how`; avoid `right` joins and
  don't paper over duplicates with `distinct()`/`dropDuplicates()`.
- Keep logical expressions to ≤3 ops (extract named conditions); files ≤~250 lines,
  functions ≤~70 lines, chains ≤5 expressions; wrap multi-line chains in one paren pair
  (no `\`). Import aliases are fixed: `from pyspark.sql import types as T, functions as F`.
