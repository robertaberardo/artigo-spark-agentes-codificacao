# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PySpark 3.5.4 + Apache Sedona 1.9.0 (+ GDAL/PROJ) geospatial data pipelines over
Brazilian public-sector datasets. A "validation environment": jobs are written to
run inside a Docker container against an S3-compatible object store (MinIO), reading
and writing a data lake organized as a **medallion architecture** (`raw → bronze →
silver → gold`).

## Language convention (enforced)

- **Code, identifiers, and column names: English.** Column names in transformed
  layers (bronze onward) are `snake_case`.
- **Docstrings and comments: Portuguese.** Follow this — it is consistent across the
  whole codebase.

## Read first

`CODING_STANDARDS.md` is authoritative for all PySpark code (a Portuguese adaptation
of the Palantir PySpark Style Guide). Key rules that shape the architecture, not just
style:
- Model each step as a pure `DataFrame -> DataFrame` or `Column -> Column` function;
  chain with `.transform(...)`. **Check `utils/` for an existing helper before writing
  a new generic transform** — new generic transforms belong in `utils/`, not in the job body.
- **No UDFs.** Use native `pyspark.sql.functions` / Sedona API functions. Prefer
  `F.native_fn(...)` over `F.expr("...")`.
- Absence is `F.lit(None)`, never `""` / `"NA"` / `0`. Do not null-fill before aggregating.
- Geodesic distance (`ST_DistanceSpheroid` on WGS84) for meters — **measure on the
  original 4326 point before projecting**; never round-trip through 3857 just to measure.
  3857 is an output convention only.

## Architecture

Each dataset source is an **app** under `apps/<app>/` (currently: `ana`, `anac`,
`bcb`, `caged`, `cvm`, `datasus`, `dnit`, `educacao`, `ibge`, `inmet`, `snis`, `tse`).
Every app has the same shape:

```
apps/<app>/
  metadata.toml            # declares every table, its layers, key, and per-layer schema
  jobs/bronze/*.py         # one job per table; raw CSV -> typed/cleaned bronze
  jobs/silver/*.py         # joins, geo-referencing, consolidation across bronze tables
  jobs/gold/*.py           # aggregations, rankings, H3 hexbins for analytics
  utils/column_transforms.py   # domain-specific column helpers for THIS app only
```

Each job is a standalone `spark-submit` entrypoint with a `main()`: it reads one or
more source tables, applies transforms, and **persists the result** to the layer
matching the transform's nature. Layer dependency flows strictly `raw → bronze →
silver → gold` (a gold job reads silver, silver reads bronze, bronze reads raw CSV).

### Shared code (`utils/`, importable as `from utils import ...`)

- `utils/column_transforms.py` — `Column -> Column` helpers (BR number/date/money
  parsing, null handling, arrays/maps/structs, Sedona geometry fixers, keys).
- `utils/dataframe_transforms.py` — `DataFrame -> DataFrame` helpers (coordinate
  validation for Brazil bounds, point/line geometry, reprojection, H3 indexing &
  aggregation, window rankings/moving-averages, safe joins like `join_no_fanout`).
- `utils/metadata.py` — **`table_location(app, table, level)`** builds the S3A path
  from the app's `metadata.toml`. Jobs must not hand-build lake paths; get them here.
  It raises early if the table/level isn't declared. `PYTHONPATH` is set to the project
  root so `from utils import ...` and `from apps.<app>... import ...` resolve.

### metadata.toml is the schema contract

Table location (`s3a://{db}/{app}/{name}/{level}`), key, available `levels`, and the
**explicit schema per layer** all live in `metadata.toml`. New tables or layers are
declared there first. `S3_BUCKET` env var overrides `data_lake.db` for the bucket.
Jobs read with an explicit `StructType` schema (never `inferSchema`); lat/long are
`double`, never string.

### Spark vs. Sedona context

- Non-geo jobs: `SparkSession.builder.appName(...).getOrCreate()`.
- Geo jobs (anything using `ST_*` / geometry / GeoParquet): must create a Sedona
  context — `SedonaContext.create(SedonaContext.builder().appName(...).getOrCreate())`
  — or the spatial functions are not registered. Geo outputs are written as
  `format("geoparquet")`; H3 grids and persisted geometries are in `EPSG:3857`.

## Running

Everything runs in the Docker container defined by `Dockerfile` / `docker-compose.yml`
(Fedora 39, Java 11 headless, Python 3.11 venv at `/opt/venv`, GDAL 3.8.1 built from
source). The project root is mounted at `/workspace/spark-project`; Python sources are
mounted at runtime, so editing a job does not require a rebuild.

```bash
# The compose file attaches to an EXTERNAL docker network named `external-net`
# (the MinIO service `s3` lives there). Create it once if missing:
docker network create external-net

docker compose up -d --build          # build image + start the `spark` container
docker compose exec spark bash        # shell inside the container

# Run a single job (paths are relative to the mounted project root):
docker compose exec spark spark-submit apps/ana/jobs/bronze/ana_estacao.py
docker compose exec spark spark-submit apps/ana/jobs/silver/ana_estacao_geo.py
```

Config comes from env (`.env`, see `.env.example`): S3 endpoint/bucket and
`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`. S3A wiring lives in
`conf/spark-defaults.conf` (loaded via `SPARK_CONF_DIR`) — keep connection config out
of job code. Spark UI on `4040` (host `14040`), Jupyter on `8888` (host `18888`).

There is no separate build/lint/test toolchain; "test" here means running the job (or
its transforms) and verifying the output data, per `CODING_STANDARDS.md`.

## Adding a job (typical flow)

1. Declare the table + per-layer schema in the app's `metadata.toml`.
2. Reuse a `utils/` transform if one fits; if you need a new *generic* one, add it to
   `utils/` (column-level vs. DataFrame-level module), not the job. Domain-specific
   logic goes in `apps/<app>/utils/`.
3. Resolve paths via `table_location(...)`; read with an explicit schema.
4. Write the result to the appropriate layer and print a row count.
