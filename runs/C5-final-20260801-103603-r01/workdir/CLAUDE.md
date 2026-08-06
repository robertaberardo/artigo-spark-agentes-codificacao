# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A monorepo of PySpark batch pipelines that ingest and transform Brazilian public-data
sources (ANAC, DATASUS, IBGE, TSE, BCB, INMET, DNIT, CAGED, CVM, SNIS, ANA, educação).
Each source is an **app** under `apps/<app>/`. Pipelines follow the **medallion
architecture** (`raw` → `bronze` → `silver` → `gold`) and read/write Parquet on an
S3-compatible object store (MinIO/S3A). Geospatial work uses **Apache Sedona** + GDAL/PROJ.

## Architecture

### Apps and the job layout
Every app is self-contained and mirrors the same structure:

```
apps/<app>/
  metadata.toml              # table registry: locations, keys, per-layer schemas
  jobs/{bronze,silver,gold}/ # one spark-submit script per output table
  utils/column_transforms.py # transforms specific to this app's domain
```

A **job** is a standalone script with a `main()` and `if __name__ == "__main__"`, run
via `spark-submit`. It reads one or more source tables, transforms, and **persists the
result to the layer matching the transformation** (a job always ends by writing to the
data lake). The layer directory (`bronze/silver/gold`) reflects the output layer.

- **bronze** — read `raw` (CSV, `;`-separated, explicit `StructType` schema), standardize
  column names to `snake_case` via `.alias()`, clean/cast types, filter invalid keys.
- **silver** — join/consolidate bronze tables, resolve lookups, derive columns. Geo
  jobs create a `SedonaContext` here.
- **gold** — aggregate to analytics-ready tables (rankings, per-UF rollups, H3 hexbins).

### Shared vs. app-specific transforms
Reusable, source-agnostic transforms live in the top-level `utils/` (importable as
`utils.*` because `PYTHONPATH` = project root):

- `utils/column_transforms.py` — pure `Column -> Column` functions (text normalization,
  BR number/date/money parsing, null handling, coordinates, arrays/maps, Sedona geometry
  helpers, window helpers, keys).
- `utils/dataframe_transforms.py` — pure `DataFrame -> DataFrame` functions (joins,
  dedup, H3 gridding, reprojection, distance-to-reference, ranking/window rollups, etc.).
- `utils/metadata.py` — loads `metadata.toml` and resolves table paths.

**Before writing a new generic transform, check `utils/` for an existing one and reuse
it.** New *generic* transforms belong in `utils/`, not in a job body. Transforms tied to
one source (e.g. `is_valid_oaci` for ANAC) go in that app's `utils/column_transforms.py`.

### Table locations come from metadata, not string literals
`metadata.toml` is the source of truth for where each table lives and what schema each
layer has. Resolve paths with `utils.metadata.table_location(app, table, level)`, which
returns `s3a://{bucket}/{app}/{table_name}/{level}` and **fails fast** if the table or
layer isn't declared. The bucket comes from `S3_BUCKET` (default `datalake`).

```python
from utils.metadata import table_location
ORIGEM  = table_location("anac", "aerodromo", "raw")
DESTINO = table_location("anac", "aerodromo", "bronze")
```

New tables and layers must be declared in the app's `metadata.toml` first — including
the schema per layer (`[[tables.<t>.<level>.schema]]` entries with `name`/`type`/
`description`) — before a job references them.

> Note: some existing jobs still hardcode `s3a://...` paths and inline schemas instead of
> using `table_location`/metadata. The metadata-driven form is the intended pattern —
> prefer it for new work.

## Coding standards

`CODING_STANDARDS.md` is the authoritative style guide (a Brazil-adapted Palantir PySpark
guide) and **must be followed** for all PySpark code. It is enforced by review, not tooling
— there is no linter/formatter configured. Highlights that shape most edits:

- **Language:** code, function/variable names, and column names in **English**; docstrings
  and comments in **Portuguese**. Comment the *why*, not the *what*.
- **Transformed columns are `snake_case`**; standardize to lowercase in the bronze `select`.
- **Model each step as a pure `DataFrame -> DataFrame` (or `Column -> Column`) function**
  and chain with `.transform(...)` rather than reassigning per step.
- **No UDFs** — rewrite with native `pyspark.sql.functions`. Prefer native
  `F.something(...)` over `F.expr("...")`; reference columns with `F.col("name")`.
- **`select` is the schema contract** — do casts inside `select`, keep it to ~one function
  per column; extract to a `clean_<name>()` if it grows.
- **Absence is never zero:** fill empty columns with `F.lit(None)` (never `""`/`"NA"`), and
  don't coalesce nulls to `0` before aggregating unless the contract says so.
- **Joins:** always state `how=`; avoid `right` joins; don't paper over duplicates with
  `.dropDuplicates()`/`.distinct()` — investigate fan-out (see `join_no_fanout`).
- **Window functions:** always give an explicit frame (`rowsBetween`/`rangeBetween`); avoid
  empty `partitionBy()`.
- **Geo (Sedona):** use the Python API over hand-built SQL; compute lat/long distances as
  **geodesic** distance (`ST_DistanceSpheroid` on WGS84) measured on the **original
  coordinates before projecting**; store output geometry in EPSG:3857; lat/long are always
  `double`. A `SedonaContext` must be active for any `st_*` helper.
- Files ≲250 lines, functions ≲70 lines; chains ≤5 expressions; imports aliased as
  `from pyspark.sql import types as T, functions as F`.

## Running jobs

Everything runs inside the Docker image (Fedora 39 + Java 11 + Python 3.11 + GDAL 3.8.1/
PROJ 9.3.1 + Sedona/S3A jars baked in). The project root is bind-mounted at
`/workspace/spark-project`, so Python sources are live-editable without rebuilding.

```bash
# Build + start the long-lived container (needs an external docker network `external-net`)
docker compose up -d --build

# Run a single job (paths are relative to the mounted project root)
docker compose exec spark spark-submit apps/anac/jobs/bronze/anac_aerodromo.py

# Open a shell in the container
docker compose exec spark bash
```

Config/credentials are kept out of job code:
- `conf/spark-defaults.conf` (read via `SPARK_CONF_DIR`) holds the S3A/MinIO plumbing.
- S3 endpoint/bucket and AWS credentials come from env vars (`.env`, see `.env.example`);
  the S3A connector reads them via `EnvironmentVariableCredentialsProvider`.
- Spark UI is exposed on host port `14040` (`SPARK_UI_PORT`), Jupyter on `18888`.

There is **no automated test suite** in the repo. "Testing" means running the job against
the data lake and verifying the output (jobs print a row count and often `.show()` a
sample after writing). The Dockerfile pins the exact runtime versions
(pyspark 3.5.4, sedona 1.9.0, Java 11, Python 3.11) — Sedona 1.9 requires Java 11+, and
pyspark 3.5 does not support Python 3.12, so those pins are load-bearing.
