# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PySpark + Apache Sedona geospatial data pipelines over Brazilian public datasets,
organized as a **medallion lake** (raw → bronze → silver → gold) on an S3-compatible
object store (MinIO). Each top-level dir under `apps/` is one data source
(`ana`, `anac`, `bcb`, `caged`, `cvm`, `datasus`, `dnit`, `educacao`, `ibge`,
`inmet`, `snis`, `tse`). Everything runs inside a Docker container; there is no
local Python environment.

**`CODING_STANDARDS.md` is mandatory reading before writing or reviewing any
PySpark code.** It is a Palantir-style guide adapted to this repo (pure
`DataFrame -> DataFrame` transforms, no UDFs, native `F.*` over `F.expr`,
`F.lit(None)` for absence, explicit join `how`, explicit window frames, etc.).
The rules below are the architecture; that file is the code style.

## Language convention

- **Code, identifiers, and column names: English.**
- **Docstrings and comments: Portuguese.** Match this when editing existing files.
- Transformed column names (bronze onward) are `snake_case`; raw reads reference
  the source's original column casing and alias to lowercase in the bronze `select`.

## Architecture

### Layers (what each job does)
- **bronze** (`apps/<app>/jobs/bronze/`): reads raw CSV with an **explicit
  `StructType`** (never `inferSchema`), `sep=";"`, `header=True`; cleans/casts via
  `utils` column transforms; writes **parquet**. Raw is all-`string`; bronze
  applies real types.
- **silver** (`apps/<app>/jobs/silver/`): joins/consolidates bronze tables and
  builds geometry. Geospatial silver uses `SedonaContext` and writes **geoparquet**.
- **gold** (`apps/<app>/jobs/gold/`): aggregations, rankings, and H3 hexbin
  density tables for consumption.

Every job ends by **persisting** its result to the lake and printing a
`>> <layer> <table>: <n> registros em <path>` line.

### Table locations come from metadata, never hardcoded paths
`apps/<app>/metadata.toml` is the single source of truth for every table: its
physical `name`, which `levels` (raw/bronze/silver/gold) exist, its `key`, and a
**per-level `schema`** (name/type/description). Jobs resolve paths through
`utils.metadata.table_location(app, table, level)`, which returns
`s3a://{bucket}/{app}/{table_name}/{level}`.

- The bucket is `S3_BUCKET` (env) overriding `data_lake.db` from the metadata.
- `table_location` raises `KeyError` for an undeclared table and `ValueError` for
  a level not listed in that table's `levels` — it fails early instead of reading
  an empty prefix.
- **Adding a table or a new layer means declaring it in `metadata.toml` first**
  (including the schema for that level), then writing the job.

### Shared vs. app-specific transforms
- `utils/column_transforms.py` — reusable `Column -> Column` functions
  (text/number/date normalization, arrays/maps/structs, Sedona geometry helpers,
  window helpers).
- `utils/dataframe_transforms.py` — reusable `DataFrame -> DataFrame` functions
  (coordinate validation against Brazil bounds, joins without silent fan-out,
  H3 gridding, reprojection, moving averages/rankings, flattening).
- `apps/<app>/utils/column_transforms.py` — domain-specific helpers for that
  source only (e.g. `is_valid_station_code`).

**Before writing a new generic transform, check `utils/` for an existing one and
reuse it.** New *generic* transforms belong in `utils/`, not in a job body. Jobs
chain these with `DataFrame.transform(...)`.

### Geospatial specifics (Sedona)
- Geometry jobs create Spark via
  `SedonaContext.create(SedonaContext.builder()...getOrCreate())`, not plain
  `SparkSession`.
- Distances in meters use **geodesic** `ST_DistanceSpheroid` measured on the
  **original WGS84 point, before projecting**. Persisted geometry is reprojected
  to `EPSG:3857` afterward (`to_web_mercator`); never round-trip `4326→3857→4326`
  just to measure. See the extended note in `CODING_STANDARDS.md` and the worked
  example in `apps/ana/jobs/silver/ana_estacao_geo.py`.

## Running jobs

Everything executes in the `spark` container. It depends on an **external Docker
network `external-net`** and an S3/MinIO service reachable there at `http://s3:9000`
(S3A config lives in `conf/spark-defaults.conf`, read via `SPARK_CONF_DIR`).

```bash
docker compose build            # first build compiles GDAL (~10 min, cached after)
docker compose up -d            # start the long-lived container (it just tails)

# run one job (paths are project-relative; PYTHONPATH=/workspace/spark-project)
docker compose exec spark spark-submit apps/ana/jobs/bronze/ana_estacao.py
```

Jobs are ordered by layer — a silver job reads the bronze it depends on, so run
bronze → silver → gold for a source. Spark UI is exposed on `SPARK_UI_PORT`
(default host `14040` → container `4040`).

Copy `.env.example` to `.env` for local S3 endpoint/credentials
(`admin`/`admin1234` against MinIO by default).

## Testing

There is no automated test suite. Per `CODING_STANDARDS.md`, verify changes by
running the affected job(s) via `spark-submit` and confirming the output row
count and schema match the table's declared `metadata.toml` schema. Because
transforms are pure `DataFrame -> DataFrame` / `Column -> Column` functions, they
are designed to be exercised in isolation (e.g. in a Jupyter/`pyspark` session
inside the container).
