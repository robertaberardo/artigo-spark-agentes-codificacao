# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PySpark + Apache Sedona geospatial data pipelines that ingest Brazilian public datasets
(one app per source: `ana`, `anac`, `bcb`, `caged`, `cvm`, `datasus`, `dnit`, `educacao`,
`ibge`, `inmet`, `snis`, `tse`) and refine them through a medallion architecture into an
S3-compatible data lake. `educacao` is currently an empty scaffold (no jobs yet).

The container writes to an **external** S3/MinIO object store on a Docker network named
`external-net` (see `docker-compose.yml`) — it is not part of this compose file. That
network and store must exist before `docker compose up`.

## Running jobs

Everything runs inside the `spark` container (Fedora 39, Java 11, Python 3.11 venv at
`/opt/venv`, Spark/Sedona/S3A jars pre-baked into the image). The project root is
mounted at `/workspace/spark-project`, which is both the build context and the working
directory — Python sources are picked up live without rebuilding.

```bash
docker compose up -d --build          # build image + start container (idles via tail -f)
docker compose exec spark bash        # shell into the container

# inside the container (cwd = project root, PYTHONPATH already set):
spark-submit validate_setup.py                       # smoke-test the full stack
spark-submit apps/<app>/jobs/<layer>/<job>.py        # run one job
```

Rebuild the image only when `Dockerfile`, `pyproject.toml`, or `docker/resources/*.sh`
change. The GDAL/PROJ compile (~10 min) is an isolated early layer, so editing
`pyproject.toml` re-runs only the dependency layer, not GDAL.

There is no test suite or linter configured. "Testing" means running the job and
verifying the output row count / data (each job prints a `>>` summary and re-reads its
output to count rows). Jupyter is available for table exploration on port `18888`
(host), Spark UI on `14040`.

## Medallion architecture

Data flows **raw → bronze → silver → gold**, one job (`main()`) per table per layer,
under `apps/<app>/jobs/<layer>/`:

- **raw** — source files as they land (CSV, `sep=";"`, Brazilian formats: `,` decimals,
  `dd/MM/yyyy` dates, lat/long as text). Never produced by a job; read with an explicit
  `StructType` (all `string`), never `inferSchema`.
- **bronze** — typed and cleaned. Column names lowercased to `snake_case` here, in the
  first `select` via `.alias(...)`. Written as Parquet.
- **silver** — joined/enriched/georeferenced. Geometry tables are written as
  `geoparquet` in **EPSG:3857**.
- **gold** — aggregations, rankings, H3 hexbins — analytics-ready outputs.

### Table locations come from metadata, never hardcoded paths

Each app has an `apps/<app>/metadata.toml` declaring every table: its `name`, which
`levels` exist, the `key`, and a per-level `schema` (name/type/description). Jobs resolve
paths through `utils.metadata.table_location(app, table, level)`, which builds
`s3a://{bucket}/{app}/{table_name}/{level}` and **raises** if the table or level isn't
declared (fail early instead of reading an empty prefix). The bucket comes from
`$S3_BUCKET` (overriding `data_lake.db` in the toml) for parity with production.

**When adding a table or layer, declare it in `metadata.toml` first** (including its
per-layer schema), then write the job.

## Shared transforms — check before writing new logic

Generic, reusable transforms live in two top-level modules; **search these before
writing any new transform**:

- `utils/column_transforms.py` — pure `Column -> Column` functions (text/number/date
  parsing, null handling, arrays, window helpers, Sedona geometry column ops).
- `utils/dataframe_transforms.py` — `DataFrame -> DataFrame` functions (joins, unions,
  window analytics, H3 gridding, reprojection, flattening).

Domain-specific helpers that aren't reusable across apps go in
`apps/<app>/utils/column_transforms.py` (e.g. `ana`'s `is_valid_station_code`). A job
imports both: `from utils import column_transforms as ct` and
`from apps.<app>.utils import column_transforms as <app>_ct`.

Jobs compose these via `DataFrame.transform(...)` chains rather than reassigning at each
step. New *generic* transforms belong in `utils/`, not inline in a job.

## Job shape (follow the existing pattern)

A job is a module with a `main()` that: builds the session, reads its input(s), applies a
`.transform(...)` / `.select(...)` chain, writes with `mode("overwrite")`, re-reads to
print a count, and `spark.stop()`. Use `SparkSession.builder` for non-spatial jobs and
`SedonaContext.create(SedonaContext.builder()...)` for anything touching geometry (Sedona
functions require an active `SedonaContext`).

## Geospatial conventions (Sedona)

- **`geometry` is stored in EPSG:3857** (Web Mercator); source lat/long is WGS84
  (EPSG:4326). `dt.to_web_mercator` / `dt.reproject_geometry` handle the projection.
- **Measure geodesic distances on the original WGS84 point BEFORE projecting to 3857.**
  Use `ST_DistanceSpheroid` (WGS84 ellipsoid). Never round-trip 4326→3857→4326 just to
  measure, and never use planar distance in degrees — both lose precision at Brazil's
  latitudes. See `apps/ana/jobs/silver/ana_estacao_geo.py` for the canonical example.
- `ST_Point(x, y)` takes **x=longitude, y=latitude** — mind the order.
- Prefer the Sedona Python API (`st_functions`, `st_constructors`, `st_predicates`) over
  hand-built SQL, same as the `F.expr` rule below.

## Coding standards

`CODING_STANDARDS.md` (Portuguese, based on the Palantir PySpark Style Guide) is
authoritative and detailed. The rules that most shape the code here:

- **Language split:** code / identifiers / column names in **English**; docstrings and
  comments in **Portuguese**.
- Prefer native `F.<fn>(...)` over `F.expr("...")`; reference columns with `F.col("name")`
  (attribute access `df.col` only to disambiguate joins).
- **No UDFs** — rewrite with native functions.
- Model steps as pure `DataFrame -> DataFrame` / `Column -> Column` functions; a `select`
  is the schema contract at the boundaries; cast inside the `select`, not with `withColumn`.
- **Absence is never zero:** fill missing with `F.lit(None)`, never `""` or `"NA"`; don't
  turn nulls into `0` before aggregating (`mean`/`sum` already skip nulls).
- **Latitude/longitude are always `double`**, never string.
- Explicit `how` on every join; avoid `right` joins; don't paper over unexpected
  duplicates with `dropDuplicates`/`distinct` (use `dt.join_no_fanout` to make fan-out an
  error). Window functions need an explicit frame (`rowsBetween`/`rangeBetween`).
- Keep files ≲250 lines, functions ≲70 lines, chains ≲5 expressions; extract named
  conditions instead of packing >3 logical ops into one expression.
