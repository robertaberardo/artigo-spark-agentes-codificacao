# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A collection of PySpark batch pipelines that ingest Brazilian public-sector open data into an S3-compatible data lake (MinIO), following a **medallion architecture**: `raw → bronze → silver → gold`. Geospatial work uses Apache Sedona + GDAL. Each top-level dir under `apps/` is one data source (`anac`, `datasus`, `ibge`, `tse`, `bcb`, `caged`, `cvm`, `dnit`, `educacao`, `inmet`, `snis`, `ana`).

There is no application server and no test suite — jobs are standalone `spark-submit` scripts. "Running" a job means submitting it to Spark inside the Docker container; "testing" means running it and inspecting the output (see `CODING_STANDARDS.md`).

## Environment & commands

Everything runs inside the `spark` container (Fedora 39, Java 11, Python 3.11, PySpark 3.5.4, Sedona 1.9.0, GDAL 3.8.1). The repo is bind-mounted at `/workspace/spark-project`; `PYTHONPATH` and `SPARK_CONF_DIR` point there.

The container joins an external Docker network named `external-net` and expects an S3 endpoint reachable as `s3:9000` (MinIO). Both must exist before `up`.

```bash
docker network create external-net          # once, if missing
docker compose up -d --build                 # build image + start container (idle; runs `tail -f`)

# Run a single job (this is the primary dev loop):
docker compose exec spark spark-submit apps/anac/jobs/bronze/anac_aerodromo.py

# Interactive shell / Jupyter (dev extra):
docker compose exec spark bash
```

Spark UI: `localhost:14040`. Jupyter: `localhost:18888`. Ports and S3 creds come from `.env` (copy `.env.example`).

Note: `start.sh` prints hints referencing `validate_setup.py` and `exploration/start_jupyter.sh`, which are **not present** in the repo — ignore those.

## Layout of an app

Every app under `apps/<app>/` follows the same shape:

```
apps/<app>/
  metadata.toml              # table registry: names, levels, keys, per-level schemas
  jobs/{bronze,silver,gold}/ # one .py per output table
  utils/column_transforms.py # domain-specific Column helpers for THIS app only
```

A job reads from the layer below, transforms, and **persists the result** as Parquet (or geometry) into the next layer. Jobs are self-contained scripts with a `main()` that builds a Spark/Sedona session, writes output, prints a `>> <layer> <table>: <n> registros` line, and stops the session.

- **bronze** jobs read raw CSVs (explicit schema, `sep=";"`, header) and standardize columns.
- **silver** jobs join/enrich across bronze tables; geospatial silver/gold jobs use `SedonaContext.create(...)` instead of a plain `SparkSession`.
- **gold** jobs aggregate/rank into analytics-ready tables (per-UF rollups, rankings, H3 hexbins).

## `metadata.toml` is the source of truth for paths & schemas

Jobs must **not** hand-build S3 paths. `utils/metadata.py::table_location(app, table, level)` reads `metadata.toml` and returns `s3a://{bucket}/{app}/{name}/{level}`, where the bucket is `S3_BUCKET` (env) overriding `data_lake.db`. It raises if the table or level isn't declared — failing early instead of reading an empty prefix.

When adding a table or a new layer, **declare it in `metadata.toml` first** (its `name`, `levels`, `key`, and the per-level `schema` array), then write the job. The raw schema keeps the source's original column casing; bronze renames to `snake_case` via `.alias(...)`.

Caveat: some existing jobs (e.g. `apps/anac/jobs/silver/anac_voo_consolidado.py`) hardcode `s3a://...` paths instead of calling `table_location`. Prefer `table_location` for new code.

## Shared vs. app-local transforms

Two shared modules at the repo root, reused by all apps — **check here before writing a new generic transform**:

- `utils/column_transforms.py` — `Column → Column` helpers (`normalize_text`, `standardize_uf`, `to_coordinate`, `br_decimal_to_double`, `safe_divide`, `make_point`, `fix_geometry`, `degrees_to_meters`, `zero_pad_code`, `yes_no_to_int`, …).
- `utils/dataframe_transforms.py` — `DataFrame → DataFrame` helpers (`join_lookup`, `join_no_fanout`, `deduplicate_by_key`, `add_point_geometry`, `add_distance_to_reference`, `attach_h3_index`/`aggregate_by_h3`, `to_web_mercator`, `rank_within_group`, …).

Anything source-specific (e.g. `is_valid_oaci` for ANAC) goes in that app's `utils/column_transforms.py`, not the shared modules. Compose transforms with `df.transform(...)`, not by reassigning through intermediate variables.

Standard import aliases (enforced): `from pyspark.sql import types as T, functions as F`.

## Coding standards — read `CODING_STANDARDS.md`

`CODING_STANDARDS.md` (Portuguese, adapted from the Palantir PySpark Style Guide) is authoritative and detailed. The load-bearing rules:

- **Language convention:** code/identifiers/column names in English; docstrings and comments in Portuguese.
- **No UDFs**; prefer native `F.<fn>(...)` over `F.expr("...")` (comment the *why* on the rare exception).
- **Absence is never zero:** fill empty columns with `F.lit(None)`, never `""`/`"NA"`; don't coalesce nulls to `0` before aggregating unless the contract says absence counts as zero.
- **`select` is the schema contract** — one function per column plus optional `.alias()`; cast inside `select`, not via `withColumn`. Latitude/longitude are always `double`.
- **Joins:** always state `how`; avoid `right` (invert and use `left`); never use `.distinct()`/`.dropDuplicates()` to paper over unexpected duplicates.
- **Window functions:** always specify an explicit frame (`rowsBetween`/`rangeBetween`); avoid empty `partitionBy()`.
- **Geospatial (Sedona):** use the Python API over hand-written SQL; measure geodesic distance (`ST_DistanceSpheroid`, WGS84) on the **original** lat/long *before* projecting — never round-trip through EPSG:3857 just to measure. Geometry is written out in EPSG:3857 as an output convention.
- Files ≤ ~250 lines, functions ≤ ~70 lines; no commented-out code; no bare literals in filters/new columns.
