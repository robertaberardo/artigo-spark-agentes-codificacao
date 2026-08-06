# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A collection of PySpark batch pipelines that ingest and refine Brazilian public
open-data sources (ANAC, DATASUS, IBGE, TSE, BCB, CAGED, CVM, DNIT, INMET, ANA,
SNIS, educação). Data lands in and is written back to an S3-compatible object
store (MinIO in dev) as Parquet, following a **medallion architecture**:
`raw → bronze → silver → gold`.

Geospatial work uses **Apache Sedona** (Spark) + **GDAL/PROJ**; the standard
output CRS is EPSG:3857 (Web Mercator), while distances are measured on the
original WGS84 coordinates (see `CODING_STANDARDS.md` → Geoespacial).

## Language convention (important)

- **Code, function/variable names, and column names: English.**
- **Docstrings and comments: Portuguese (pt-BR).**

Match this when adding code. Column names in transformed data (bronze onward) are
`snake_case` lowercase; raw reads reference the source's original column casing
and alias to lowercase in the bronze `select`.

## Coding standards

`CODING_STANDARDS.md` is the authoritative style guide (adapted Palantir PySpark
guide) and is mandatory reading before writing job code. Non-obvious rules that
shape the architecture:

- **No UDFs** — express everything with native `pyspark.sql.functions`. Prefer
  `F.native_fn(...)` over `F.expr("...")`; same rule for Sedona's Python API over
  hand-written spatial SQL.
- Model each step as a pure `DataFrame -> DataFrame` or `Column -> Column`
  function and chain with `.transform(...)`. **Check `utils/` for an existing
  transform before writing a new generic one**; new generic transforms belong in
  `utils/`, not in the job body.
- Absence is `F.lit(None)`, never `""`/`"NA"`/`0`. Don't zero-fill nulls before
  aggregating (distorts means). `coalesce_zero` exists but is only for contracts
  that explicitly define absence as zero.
- Explicit `how` on every join; no `right` joins; never mask duplicates with
  `.dropDuplicates()`/`.distinct()` — investigate the cause (see
  `join_no_fanout`).
- Window functions must declare an explicit frame (`rowsBetween`/`rangeBetween`).
- Explicit schemas, never `inferSchema`. Latitude/longitude are always `double`.
- Import aliases are fixed: `from pyspark.sql import types as T, functions as F`.

## Architecture

### App layout

Each data source is an "app" under `apps/<app>/`:

```
apps/<app>/
  metadata.toml              # table catalog: locations, layers, per-layer schema, keys
  jobs/{bronze,silver,gold}/ # one file per output table; each has a main() entrypoint
  utils/column_transforms.py # domain-specific Column transforms for that app
```

A job reads one or more upstream tables, applies transforms, and **persists** the
result to its layer as Parquet (`.write.mode("overwrite").parquet(...)`), then
prints a row count. Layer meaning: bronze = typed/cleaned single source; silver =
joined/consolidated (often geospatial, using `SedonaContext`); gold = aggregated
analytics (rankings, per-UF rollups, H3 hexbins).

### `metadata.toml` is the schema + location contract

`metadata.toml` declares every table's `name`, available `levels`, natural `key`,
and the **per-layer column schema** (name/type/description). Table paths resolve
to `s3a://{S3_BUCKET}/{app}/{table_name}/{level}` via
`utils.metadata.table_location(app, table, level)`. New tables/layers must be
declared here first — jobs are not supposed to build paths by hand.

> **Note / inconsistency to respect:** bronze jobs use `table_location(...)`, but
> many silver/gold jobs currently hardcode `s3a://...` string constants. Per the
> standards, **prefer `table_location(...)`**; when editing a job that hardcodes
> paths, migrating it to `table_location` is an improvement, not a regression.

### Shared `utils/` (repo root, on `PYTHONPATH`)

- `utils/column_transforms.py` — reusable `Column -> Column` expressions
  (text/date/number normalization, BR decimal/money parsing, coordinate parsing,
  null-safe helpers, Sedona geometry validation, keys). Imported as
  `from utils import column_transforms as ct`.
- `utils/dataframe_transforms.py` — reusable `DataFrame -> DataFrame` ops (joins,
  unions, dedup, window rankings, coordinate filtering, Sedona reprojection/H3,
  flatten). Imported as `from utils import dataframe_transforms as dt`.
- `utils/metadata.py` — `load_metadata` / `table_location`.

Sedona-dependent transforms require an active `SedonaContext`; jobs that use them
create the session with `SedonaContext.create(SedonaContext.builder()...)` instead
of a plain `SparkSession`.

## Running jobs

Everything runs inside the Docker container (Fedora 39, Java 11, Python 3.11,
PySpark 3.5.4, Sedona 1.9.0, GDAL 3.8.1). The project root is mounted at
`/workspace/spark-project`; `PYTHONPATH` and `SPARK_CONF_DIR` point there so
`from utils...` imports and S3A config resolve.

```bash
# Bring up the container (expects an external Docker network `external-net`
# and a reachable S3/MinIO at S3_ENDPOINT; copy .env.example -> .env first)
docker compose up -d --build

# Run a single job (paths are relative to the mounted project root)
docker compose exec spark spark-submit apps/<app>/jobs/<layer>/<job>.py

# e.g.
docker compose exec spark spark-submit apps/anac/jobs/bronze/anac_aerodromo.py
```

Ports: Spark UI on `${SPARK_UI_PORT:-14040}` → 4040, Jupyter on
`${JUPYTER_PORT:-18888}` → 8888. S3A talks to `S3_ENDPOINT` (path-style, no SSL)
using `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` from the environment.

## Testing

There is no automated test suite in the repo. The standards call for verifying
jobs by running them locally and confirming the output data (row counts / spot
checks) is as expected. There is a per-job convention of printing a count and
sometimes `.show()` after writing.

## Dependencies

Managed in `pyproject.toml` (Python `==3.11.*`, pinned `pyspark==3.5.4`,
`apache-sedona==1.9.0`). The Spark/Sedona/S3A JARs are pre-downloaded into
pyspark's `jars/` at image build time (see `docker/resources/setup-env.sh`) so
Spark does not resolve them via Ivy at runtime — if you change Spark/Sedona/Hadoop
versions, update the JAR URLs there to match.
