# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A collection of PySpark + Apache Sedona geospatial ETL pipelines over Brazilian
public-data sources (ANA, ANAC, BCB, CAGED, CVM, DATASUS, DNIT, educação, IBGE,
INMET, SNIS, TSE). Everything runs against an S3-compatible object store (MinIO)
using a medallion **raw → bronze → silver → gold** data-lake architecture.

## Read before writing code

**`CODING_STANDARDS.md` is authoritative** — a PySpark style guide (adapted from
Palantir's) that this repo follows strictly. Read it before writing or reviewing
any job or util. The rules that most shape the code:

- **Bilingual convention: code/identifiers/column names in English; docstrings
  and comments in Portuguese.**
- Column names in transformed layers are `snake_case`, lowercased; the raw read
  keeps the source's original casing and aliases to `snake_case` in the bronze
  `select`.
- Model each step as a pure `DataFrame -> DataFrame` or `Column -> Column`
  function and chain with `.transform(...)`. **Check `utils/` before writing a
  new generic transform** — new generic ones belong in `utils/`, not in the job.
- **No UDFs.** Prefer native `F.<fn>(...)` over `F.expr("...")`.
- Missing data is always `F.lit(None)` — never `""`, `"NA"`, or `0`. Absence is
  not zero.
- Explicit schemas (never `inferSchema`); lat/long always `double`.
- Geodesic distance (`ST_DistanceSpheroid` on WGS84) measured on the **original**
  coordinates **before** projecting to EPSG:3857 — never round-trip 4326→3857→4326
  to measure.

There is no linter/formatter config and no automated test suite in the repo.
"Testing" per the standard means running the job or manually verifying output.

## Architecture

### Per-app layout (every `apps/<app>/` is identical in shape)

```
apps/<app>/
  metadata.toml              # schema + location contract for ALL tables
  utils/column_transforms.py # domain-specific Column->Column helpers for this app
  jobs/bronze/<table>.py     # raw CSV -> cleaned bronze (parquet)
  jobs/silver/<table>.py     # bronze -> enriched/geo silver (geoparquet)
  jobs/gold/<table>.py       # silver -> aggregates/rankings/hexbins
```

Each job file is a **standalone `spark-submit` script** with a `main()`: it
reads its input table, applies chained `.transform(...)` steps, and writes the
result. Layers are a dependency chain — gold reads silver, silver reads bronze,
bronze reads raw — so they must be run in order.

### Shared code (`utils/` at repo root — imported as `from utils import ...`)

- **`utils/metadata.py`** — `table_location(app, table, level)` is the single
  source of table paths. Jobs never build S3 paths by hand. It reads the app's
  `metadata.toml`, returns `s3a://{bucket}/{app}/{table_name}/{level}`, and
  raises early if the table/level isn't declared. Bucket comes from `S3_BUCKET`
  (falls back to `data_lake.db`).
- **`utils/column_transforms.py`** — generic `Column -> Column` helpers (text
  normalization, BR number/date/money parsing, null handling, arrays/maps,
  Sedona geometry validation, window expressions, keys).
- **`utils/dataframe_transforms.py`** — generic `DataFrame -> DataFrame` helpers
  (joins without silent fan-out, unions, geo: point/line/area, reprojection, H3
  gridding & aggregation, window analytics, flatten nested/JSON).

App-level `apps/<app>/utils/column_transforms.py` holds only helpers specific to
that domain (e.g. `is_valid_station_code` for ANA). Prefer root `utils/` for
anything reusable across apps.

### `metadata.toml` is the schema contract

Every table declares its `name`, `key`, the `levels` it exists at, and a
per-level `schema` (column `name`/`type`/`description`). New tables/layers must
be declared here first. Job code uses these declarations via `table_location`;
schemas in job files (the `RAW_SCHEMA` `StructType`) must match the metadata.

### Spark session: geo vs. non-geo

- Non-geo bronze jobs: `SparkSession.builder.appName(...).getOrCreate()`.
- Any job using Sedona `st_` functions (most silver/gold geo jobs):
  `SedonaContext.create(SedonaContext.builder().appName(...).getOrCreate())`.
  Sedona spatial functions (`stc.*`, `stf.*`) require an active `SedonaContext`.

### I/O conventions

- Read raw CSVs with `.option("header", True).option("sep", ";").schema(...)`.
- Non-geo tables: `.write.mode("overwrite").parquet(dest)`.
- Geo tables (geometry columns): `.write.mode("overwrite").format("geoparquet").save(dest)`.
- Jobs typically re-read the output and `print` a row count as a sanity check.
- Persisted geometries are stored in EPSG:3857 (Web Mercator).

## Running jobs

Everything runs inside the Docker container (Fedora 39, Java 11, Python 3.11
venv, GDAL 3.8.1/PROJ 9.3.1, and Sedona/Hadoop-AWS jars pre-downloaded into
PySpark's `jars/`). The image expects a MinIO/S3 service reachable at
`http://s3:9000` on the external `external-net` Docker network.

```bash
# Build the image (GDAL compile is ~10 min the first time; cached after)
docker compose build

# Start the long-lived container (entrypoint just keeps it alive)
docker compose up -d

# Run a single job (workdir inside the container is the project root, so
# relative module paths resolve via PYTHONPATH=/workspace/spark-project)
docker compose exec spark spark-submit apps/ana/jobs/bronze/ana_estacao.py

# Run a whole app's layer in dependency order, e.g.:
docker compose exec spark spark-submit apps/ana/jobs/bronze/ana_estacao.py
docker compose exec spark spark-submit apps/ana/jobs/silver/ana_estacao_geo.py
docker compose exec spark spark-submit apps/ana/jobs/gold/ana_estacoes_hexbin.py

# Spark UI: http://localhost:14040   Jupyter: http://localhost:18888
```

S3 credentials and endpoint come from environment variables (`.env`, see
`.env.example`): `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (S3A uses
`EnvironmentVariableCredentialsProvider`), `S3_ENDPOINT`, `S3_BUCKET`. S3A wiring
lives in `conf/spark-defaults.conf` (loaded via `SPARK_CONF_DIR`).

## Adding a new table/job

1. Declare the table in `apps/<app>/metadata.toml` — every level it will exist
   at, its key, and the per-level `schema`.
2. Write the job under `apps/<app>/jobs/<layer>/`, deriving paths from
   `table_location(...)` and reusing `utils/` transforms via `.transform(...)`.
3. Keep files ≲250 lines and functions ≲70 lines; extract a `clean_<name>()`
   when a `select` gets more than ~three function-per-column expressions
   (see `CODING_STANDARDS.md`).
