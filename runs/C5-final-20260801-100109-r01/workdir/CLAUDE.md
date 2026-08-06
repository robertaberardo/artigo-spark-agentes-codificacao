# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PySpark + Apache Sedona geospatial data pipelines over Brazilian open-data sources
(ANAC, DATASUS, IBGE, TSE, DNIT, BCB, CAGED, CVM, INMET, SNIS, ANA, educação). Each
source is an **app**; each app ingests and refines its tables through a **medallion
architecture** (`raw → bronze → silver → gold`), reading and writing Parquet/GeoParquet
on an S3A-compatible object store (MinIO in dev).

`CODING_STANDARDS.md` is the authoritative PySpark style guide (Palantir-derived) and
takes precedence over general habits — **read it before writing job code.** Key rules
are summarized under "Conventions" below, but the file has the full detail and examples.

## Language convention (non-obvious, enforced)

- **Code, function/variable names, and column names: English.**
- **Docstrings and comments: Portuguese.**
- Transformed columns (bronze onward) are `snake_case`. Raw reads reference the
  **original** source column names (source casing); lowercasing happens in the bronze
  `select` via `.alias(...)`.

## Environment & commands

Everything runs inside the Docker container — Java 11, Python 3.11, GDAL/PROJ, and the
Spark/Sedona/S3A jars are all baked into the image. There is **no local `pip install`
workflow**; the host has no Spark. `apps/`, `utils/`, and `conf/` are bind-mounted at
runtime, so code edits take effect without rebuilding.

```bash
# Build image + start container (needs an external docker network named external-net)
docker compose up -d --build

# Run a single job (spark-submit; WORKDIR is the project root, PYTHONPATH includes it)
docker compose exec spark spark-submit apps/anac/jobs/bronze/anac_aerodromo.py

# Open a shell in the container
docker compose exec spark bash

# Spark UI → localhost:14040   Jupyter → localhost:18888  (see .env for ports)
```

There is no test runner, linter, or build step wired up. "Testing" a job means running
it and checking the printed record count / `.show()` output (each `main()` reads back
what it wrote and prints a `>> <layer> <table>: N ...` line). `docker/start.sh` and the
Dockerfile reference a `validate_setup.py` and an `exploration/` dir that are **not
present in this tree** — don't assume they exist.

Rebuild the image only when `pyproject.toml`, the GDAL script, or the setup scripts
change (the Dockerfile orders layers so GDAL's ~10-min compile stays cached across
dependency edits).

## Architecture

### One job = one file = one output table

A job is a `.py` file under `apps/<app>/jobs/<layer>/<table>.py` with a `main()` that:
1. Builds a Spark session — plain `SparkSession.builder` for non-spatial jobs, or
   `SedonaContext.create(SedonaContext.builder()...)` when any Sedona `ST_*` function
   is used (including transforms from `utils` that call Sedona).
2. Reads its input layer(s), applies pure transforms, and **writes the result**,
   `mode("overwrite")`, to its layer's path. Geospatial silver/gold write
   `format("geoparquet")`.
3. Reads the output back and prints the row count. Ends with `spark.stop()`.

Layers build strictly on the layer below (bronze reads raw, silver reads bronze, gold
reads silver). Bronze is where raw source strings become typed, cleaned columns.

### Table locations come from `metadata.toml`, not hardcoded strings

Each app has an `apps/<app>/metadata.toml` declaring every table: its physical `name`,
which `levels` exist, the `key`, and a per-level `schema` (column name/type/description).
Jobs resolve paths through `utils.metadata.table_location(app, table, level)`, which
builds `s3a://{bucket}/{app}/{name}/{level}` and **raises early** if the table or level
isn't declared. The bucket comes from `$S3_BUCKET` (overriding `data_lake.db`).

> Most jobs use `table_location(...)`; a minority still hardcode `s3a://...` constants.
> **Prefer `table_location`** for new/edited jobs, and declare new tables/layers (with
> their per-layer schema) in `metadata.toml` — never invent a path in job code.

Read schemas are explicit `StructType` in the job (no `inferSchema`); they must match
the raw schema in `metadata.toml`.

### Shared vs. app-local transforms

Transforms are modeled as **pure functions**, chained with `DataFrame.transform(...)`:

- `utils/column_transforms.py` — generic `Column → Column` helpers (text/number/date
  cleaning, Brazilian number/money/date parsing, null-safe array/map ops, coordinate
  parsing, geometry validation, keys). **Check here before writing any generic column
  logic.**
- `utils/dataframe_transforms.py` — generic `DataFrame → DataFrame` helpers (joins with
  fan-out guards, unions across schemas, window ranks/shares/moving averages,
  reprojection, H3 gridding, flatten). **Check here before writing DataFrame plumbing.**
- `apps/<app>/utils/column_transforms.py` — **domain-specific** transforms for that
  source only (e.g. `is_valid_oaci` for ANAC). New generic transforms belong in
  `utils/`, not the job body and not an app's utils.

Established import aliases (keep them): `from pyspark.sql import types as T, functions as F`.

### Geospatial specifics (Sedona)

- Use the Sedona **Python API** (`st_functions`, `st_constructors`, `st_predicates`,
  `st_aggregates`), not hand-written SQL, mirroring the "prefer native `F.x` over
  `F.expr`" rule.
- Latitude/longitude are always `double` (never string) to preserve precision.
- Distances in meters use **geodesic** `ST_DistanceSpheroid` over the **original WGS84**
  coordinates, measured *before* any projection. Storing geometry in EPSG:3857 is an
  output convention (`to_web_mercator`), not the measurement CRS — don't round-trip
  `4326→3857→4326` to measure.
- Coordinate validity is bounded to Brazil (`filter_valid_coordinates`,
  `BRAZIL_LAT/LON_*` constants in `dataframe_transforms.py`).

## Conventions (the ones most likely to bite — full list in `CODING_STANDARDS.md`)

- **No UDFs** — rewrite with native functions; prefer `F.native(...)` over `F.expr(...)`.
- Treat `select` as the schema contract at the start/end of a transform; do type casts
  inside the `select`, not via a follow-up `withColumn`.
- **Absence is `NULL`, never `""`/`"NA"`/`0`.** Don't zero-fill before aggregating —
  `F.mean/avg/sum` already ignore nulls; zero-filling distorts them. Only coalesce to `0`
  when the contract says absence *counts as* zero (see `coalesce_zero`).
- Always state `how` on joins; avoid `right` joins; never use `dropDuplicates`/`distinct`
  to paper over unexpected fan-out (there's a cause — investigate; see `join_no_fanout`).
- Window functions must have an **explicit frame** (`rowsBetween`/`rangeBetween`); avoid
  empty `partitionBy()` (use `agg` instead).
- Extract literals and multi-clause boolean logic into named variables (≤3 ops per
  block). Keep files ≲250 lines, functions ≲70, method chains ≲5 expressions. No
  commented-out code.
