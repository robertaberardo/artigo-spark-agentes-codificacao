# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PySpark + Apache Sedona geospatial data pipelines over Brazilian public-data sources, following a **medallion architecture** (`raw → bronze → silver → gold`). Data lives in an S3-compatible object store (MinIO) and is read/written via `s3a://` paths. This is a *validation environment* — there is no test suite; correctness is checked by running jobs and inspecting output (see `CODING_STANDARDS.md`).

Stack (pinned): Python 3.11, PySpark 3.5.4, Apache Sedona 1.9.0, GDAL 3.8.1 / PROJ 9.3.1, Java 11. Everything runs inside a Fedora 39 Docker image — do not assume these are installed on the host.

## Architecture

**One app per data source.** Each `apps/<source>/` is self-contained: `ana`, `anac`, `bcb`, `caged`, `cvm`, `datasus`, `dnit`, `ibge`, `inmet`, `snis`, `tse` (`educacao` is scaffolded but has no jobs yet). An app has:

- `jobs/{bronze,silver,gold}/<table>.py` — one runnable job per table per layer, each with a `main()` that builds its own `SparkSession`, reads its input(s), transforms, and **persists** the result. Bronze reads raw CSV (`header=True`, `sep=";"`, explicit schema) and writes parquet; silver/gold read parquet/geoparquet.
- `metadata.toml` — the source of truth for every table's location and per-layer schema. Declares `data_lake.{db,app}`, and for each table its `name`, `levels`, `key`, and a `[[tables.<t>.<level>.schema]]` list. **New tables and layers must be declared here first.**
- `utils/column_transforms.py` — domain-specific `Column → Column` helpers (e.g. `ana`'s `is_valid_station_code`). Generic helpers do **not** go here — see below.

**Layer conventions:**
- **bronze** — clean/standardize raw columns, cast types, filter invalid rows. Plain `SparkSession`. Column names become lowercase `snake_case` here (via `.alias()` in the `select`).
- **silver** — georeferencing, joins/consolidation, windowed features. Geometry work needs `SedonaContext.create(...)`; geometry output is written as `geoparquet` in **EPSG:3857** (Web Mercator).
- **gold** — aggregations, rankings, H3 hexbins for maps.

**Shared code lives in the top-level `utils/` package** (on `PYTHONPATH` as `utils`):
- `utils/metadata.py` → `table_location(app, table, level)` builds `s3a://{bucket}/{app}/{table_name}/{level}` from `metadata.toml`. **Jobs never hand-build paths** — always import and call this. Bucket comes from `S3_BUCKET` env (falls back to `data_lake.db`). It raises if the table/level isn't declared.
- `utils/column_transforms.py` → pure `Column → Column` functions (text/number/date cleaning, geometry predicates, keys).
- `utils/dataframe_transforms.py` → pure `DataFrame → DataFrame` functions, including the Sedona geospatial toolkit (H3 gridding, reprojection, distance, spatial joins) and window helpers (ranking, moving average, running total).

**Before writing a new generic transform, search `utils/` for an existing one and reuse it.** New *generic* transforms belong in `utils/` (column-level vs. DataFrame-level modules), never inlined in a job.

A typical silver job composes these via `.transform(...)` chains:
```python
geo = (
    estacao
    .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
    .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
    .transform(dt.to_web_mercator)
    .select(...)
)
```

## Coding standards

`CODING_STANDARDS.md` (Portuguese, Palantir-derived) is authoritative and detailed — read it before writing PySpark. Load-bearing rules that shape the whole codebase:

- **Language split:** code, function/variable/column names in **English**; docstrings and comments in **Portuguese**.
- **Columns are lowercase `snake_case` from bronze onward.** Raw reads reference the original source names; the rename happens in the bronze `select` via `.alias()`.
- **Pure `DataFrame → DataFrame` / `Column → Column` functions**, chained with `.transform(...)` — no side effects, individually testable.
- **No UDFs.** Rewrite with native PySpark functions.
- **Prefer native `F.<fn>(...)` over `F.expr("...")`** (and Sedona's Python API over hand-written spatial SQL); use `F.expr` only when no native equivalent exists, with a comment saying why.
- **Missing data is `F.lit(None)`, never `""`/`"NA"`/`0`.** Absence is not zero; don't zero-fill before aggregating.
- **Explicit schema** (no `inferSchema`); **latitude/longitude always `double`**.
- **Joins:** always state `how`; no `right` joins (flip to `left`); don't paper over duplicates with `.dropDuplicates()`/`.distinct()` — find the cause (see `dataframe_transforms.join_no_fanout`).
- **Window functions:** always specify an explicit frame (`rowsBetween`/`rangeBetween`); avoid empty `partitionBy()`.
- **Geospatial distance:** compute geodesic distance (`ST_DistanceSpheroid`, WGS84) on the **original** lat/long **before** projecting to 3857. Never round-trip `4326→3857→4326` just to measure — it loses precision. Projecting geometry to 3857 is an output convention, done last.

## Commands

Everything runs in the Docker container; there is no local Python env.

```bash
# Prereq: the compose file joins an external docker network named `external-net`
docker network create external-net    # once, if it doesn't exist

docker compose build                   # build image (GDAL compile ~10 min, cached after)
docker compose up -d                   # start container (stays alive via `tail -f`)

# Run a single job (spark-submit inside the container). Paths are relative to
# the project root, which is the container WORKDIR.
docker compose exec spark spark-submit apps/ana/jobs/bronze/ana_estacao.py
docker compose exec spark spark-submit apps/ana/jobs/silver/ana_estacao_geo.py

# Interactive shell
docker compose exec spark bash
```

Run order matters: a table's silver job reads its bronze output, gold reads silver — run upstream layers first. Jobs print a record count after writing; that count check is the primary "did it work" signal.

Ports (host, overridable via `.env`): Spark UI `14040→4040`, Jupyter `18888→8888`. S3/Spark config is injected via env (`AWS_ACCESS_KEY_ID`, `S3_BUCKET`, `S3_ENDPOINT`) and `conf/spark-defaults.conf` (S3A plumbing) — keep connection config out of job code. Copy `.env.example` to `.env` to set these.

Sedona/S3A jars are pre-downloaded into PySpark's `jars/` dir at image build (`docker/resources/setup-env.sh`) — jobs don't resolve them via Ivy at runtime.
