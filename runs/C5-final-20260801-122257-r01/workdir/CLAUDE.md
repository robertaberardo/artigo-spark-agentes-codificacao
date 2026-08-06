# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PySpark + Apache Sedona + GDAL pipelines that ingest Brazilian public-sector
open data into an S3-compatible data lake (MinIO/S3A), following a medallion
architecture (**raw → bronze → silver → gold**). Everything runs inside a
Docker container; the repo is both the build context and the runtime mount.

`CODING_STANDARDS.md` is the authoritative PySpark style guide (adapted from the
Palantir PySpark Style Guide) — read it before writing or reviewing job code.
Key conventions are summarized below, but it is the source of truth.

## Commands

The project runs entirely in Docker. There is no local Python entrypoint and no
test runner — jobs are `spark-submit` scripts, and validation is done by running
them and inspecting output/counts.

```bash
# Build the image (GDAL compile is ~10 min the first time, then cached)
docker compose build

# The external docker network must exist before `up`
docker network create external-net   # once, if missing

# Start the container (stays alive via `tail -f /dev/null`)
docker compose up -d

# Run one job — path is apps/<app>/jobs/<layer>/<job>.py, run from project root
docker compose exec spark spark-submit apps/anac/jobs/bronze/anac_aerodromo.py

# Shell into the container
docker compose exec spark bash
```

- Spark UI: `http://localhost:14040` (container `4040`). Jupyter: `18888`.
- Ports, S3 endpoint/bucket, and credentials come from `.env` (see `.env.example`).
- There is no orchestrator: layers are run manually in order (bronze before the
  silver that reads it, etc.). A job fails fast if an upstream layer is missing.

## Architecture

### Apps and the medallion flow

Each data source is an **app** under `apps/<app>/`. There are 12: `ana`, `anac`,
`bcb`, `caged`, `cvm`, `datasus`, `dnit`, `educacao`, `ibge`, `inmet`, `snis`,
`tse`. Every app has the same shape:

```
apps/<app>/
  metadata.toml              # declares every table, its layers, key, and per-layer schema
  jobs/bronze/<table>.py     # raw CSV -> typed, cleaned parquet
  jobs/silver/<table>.py     # joins / consolidation / geometry across bronze tables
  jobs/gold/<table>.py       # aggregations, rankings, hexbin — analytics-ready
  utils/column_transforms.py # column transforms specific to this domain
```

Each job is a standalone script with a `main()` guarded by `if __name__ ==
"__main__"`. It builds its own Spark session, reads its input layer(s), applies
transforms, and **always ends by persisting parquet** with
`.write.mode("overwrite").parquet(...)` — then reads it back and prints a count.

- **bronze** reads raw CSV with an **explicit schema** (never `inferSchema`),
  usually `header=True, sep=";"`, and lowercases/`snake_case`s column names in
  the `select` via `.alias(...)`. Raw column names keep the source's original
  casing; bronze onward is `snake_case`.
- **silver** joins bronze tables (via `join_lookup` / `join_no_fanout`),
  consolidates, and adds geometry.
- **gold** aggregates into rankings, per-UF summaries, and H3 hexbins.

### Table locations: `metadata.toml` + `utils/metadata.py`

Table paths are **not hardcoded** in the intended design. `utils/metadata.py`
resolves them:

```python
from utils.metadata import table_location
ORIGEM  = table_location("anac", "aerodromo", "raw")     # s3a://{bucket}/{app}/{table_name}/{level}
DESTINO = table_location("anac", "aerodromo", "bronze")
```

`table_location` reads the app's `metadata.toml`, raises `KeyError` for an
undeclared table and `ValueError` for a layer that isn't listed under that
table's `levels` — so jobs fail early instead of reading an empty prefix. The
bucket comes from `$S3_BUCKET` (falling back to `data_lake.db`).

> **Inconsistency to be aware of:** some existing jobs (e.g.
> `apps/anac/jobs/silver/anac_voo_consolidado.py`,
> `apps/datasus/jobs/gold/datasus_mortalidade_por_uf.py`) hardcode `s3a://...`
> strings instead of calling `table_location`. Prefer `table_location` +
> `metadata.toml` for new/edited jobs. New tables and layers must be declared in
> `metadata.toml`, including the schema for each layer.

### Shared utilities (`utils/` at repo root)

Reusable, **domain-agnostic** transforms live here and are on `PYTHONPATH`
(`from utils import column_transforms as ct`, `from utils import
dataframe_transforms as dt`). **Before writing a new generic transform, check
whether it already exists here.**

- `utils/column_transforms.py` — `Column -> Column` functions (text
  normalization, BR number/currency/percent parsing, null handling, coordinates,
  Sedona geometry helpers, window helpers, keys/codes).
- `utils/dataframe_transforms.py` — `DataFrame -> DataFrame` functions (joins,
  dedup, geometry construction, H3 grids/joins, reprojection, ranking/windowing,
  unions). Chain them with `df.transform(fn)`.
- `utils/metadata.py` — the `table_location` resolver above.

App-specific transforms (things that only make sense for one source, e.g.
`is_valid_oaci` for ANAC) go in that app's `utils/column_transforms.py`, not the
shared module.

### Geospatial (Sedona)

Jobs that use geometry create a **`SedonaContext`**, not a plain `SparkSession`:

```python
from sedona.spark import SedonaContext
spark = SedonaContext.create(SedonaContext.builder().appName("...").getOrCreate())
```

Spatial functions in `utils` (`ST_Point`, `ST_MakeValid`, etc.) require an
active Sedona context to be registered. Coordinates are stored as `double`;
distances use geodesic measurement on original WGS84 coordinates (see the
Sedona section of `CODING_STANDARDS.md`); output geometry convention is
EPSG:3857.

### Runtime / infra

- `Dockerfile` — Fedora 39, Java 11 (Sedona 1.9 dropped Java 8), Python 3.11
  (pinned; PySpark 3.5.x doesn't support Fedora 39's default 3.12), GDAL 3.8.1 /
  PROJ 9.3.1 compiled from source. Layer order (GDAL → deps) is deliberate so
  editing `pyproject.toml` doesn't re-trigger the slow GDAL build.
- `docker/resources/setup-env.sh` — creates the venv and **pre-downloads** the
  Sedona + hadoop-aws + aws-java-sdk jars into pyspark's `jars/` dir, so Spark
  doesn't resolve them via Ivy at runtime. Bump these versions together if you
  change the Spark/Sedona/Hadoop stack.
- `conf/spark-defaults.conf` — S3A wiring (endpoint, path-style access,
  `EnvironmentVariableCredentialsProvider`). Loaded via `SPARK_CONF_DIR`; keeps
  connection config out of job code. Credentials come from `AWS_ACCESS_KEY_ID` /
  `AWS_SECRET_ACCESS_KEY`.
- `pyproject.toml` — dependency-only wheel (`py-modules = []`); sources are
  mounted at runtime, not packaged.

## Code conventions (from CODING_STANDARDS.md)

- **Language:** code, identifiers, and column names in **English**; docstrings
  and comments in **Portuguese**.
- Model each step as a pure `DataFrame -> DataFrame` / `Column -> Column`
  function and chain with `.transform(...)`; don't reassign per step.
- **No UDFs** — use native `pyspark.sql.functions`. Prefer `F.native_fn(...)`
  over `F.expr("...")`.
- Reference columns with `F.col("name")`, not `df.name` (attribute access only
  to disambiguate joins). Cast inside `select`, not a follow-up `withColumn`.
- Treat `select` as the schema contract; keep it to ~one function per column.
- **Absence is never zero:** fill empty columns with `F.lit(None)`, never `""`
  or `0`; don't null-to-zero before aggregating unless the contract demands it.
- Always specify `how` on joins; avoid `right` joins; never use
  `dropDuplicates`/`distinct` to paper over unexpected fan-out.
- Window functions must declare an explicit frame; avoid empty `partitionBy()`.
- Keep chains ≤5 expressions, files ≤~250 lines, functions ≤~70 lines; extract
  named boolean variables for complex logical expressions.
- Standard import aliases: `from pyspark.sql import types as T, functions as F`.
