# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PySpark + Apache Sedona + GDAL geospatial data pipelines over Brazilian public-data
sources (ANAC, DATASUS, IBGE, TSE, DNIT, INMET, etc.). Data lands in an S3-compatible
object store (MinIO) as a **medallion lake**: `raw → bronze → silver → gold`. Everything
runs inside a Docker container that bundles Spark 3.5.4, Sedona 1.9.0, GDAL 3.8.1 and the
S3A + Sedona jars.

Python is pinned to **3.11** (PySpark 3.5.x does not support 3.12). Not a git repo.

## Running jobs

Jobs run via `spark-submit` inside the container. The project is bind-mounted at
`/workspace/spark-project`; `PYTHONPATH` is the project root so `from utils...` and
`from apps...` imports resolve.

```bash
docker network create external-net      # once; docker-compose expects an external net
docker compose up -d --build            # build image + start long-lived container
                                        # (entrypoint is `tail -f /dev/null`)

# run a single job (module path mirrors the file tree):
docker compose exec spark spark-submit apps/anac/jobs/bronze/anac_aerodromo.py
docker compose exec spark spark-submit apps/anac/jobs/silver/anac_voo_consolidado.py
```

Run jobs **in medallion order** — silver reads bronze parquet, gold reads silver; there is
no orchestrator, so upstream layers must be materialized first. Spark UI: `localhost:14040`.
Config from `.env` (copy `.env.example`); S3 creds reach Spark's S3A connector via the
`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` env vars (see `conf/spark-defaults.conf`).

Note: `docker/resources/start.sh` prints hints for `validate_setup.py` and
`exploration/start_jupyter.sh` — those files are **not present** in this checkout.

### Testing

There is no test runner. Each job self-verifies by writing its output, then re-reading and
`print`ing a record count (`>> bronze anac_aerodromo: N registros em ...`); gold jobs often
`.show()` the result. Verify a change by running the job and checking the count/output.

## Architecture

### Per-app layout

Each source is an app under `apps/<app>/` with an identical shape:

```
apps/<app>/
  metadata.toml              # table registry — the source of truth for paths & schemas
  jobs/{bronze,silver,gold}/ # one file = one job = one output table; each has main()
  utils/column_transforms.py # domain-specific Column->Column helpers (e.g. is_valid_oaci)
```

Shared, cross-app code lives at the repo root in `utils/`:
- `utils/column_transforms.py` — generic `Column -> Column` helpers (text/number/date
  normalization, null handling, BR-format parsing, coordinates, Sedona geometry fixes).
- `utils/dataframe_transforms.py` — generic `DataFrame -> DataFrame` helpers (joins without
  fanout, coordinate filtering, point/line geometry, H3 grids, window rankings, unions…).
- `utils/metadata.py` — `load_metadata(app)` and `table_location(app, table, level)`.

### metadata.toml drives paths and schemas

`table_location(app, table, level)` returns `s3a://{bucket}/{app}/{table_name}/{level}`,
reading the bucket from `data_lake.db` (overridable via `S3_BUCKET`) and validating that the
requested `level` is declared under the table's `levels`. **Jobs must not hand-build S3
paths** — declare the table (and its per-level schema) in `metadata.toml` and resolve paths
through `table_location`. Every table entry has a `key` and a `[[tables.X.<level>.schema]]`
list (name/type/description) that documents each layer's contract, including the raw→bronze
type promotions (string → double/int/timestamp) and Sedona `geometry(...)` types for
silver/gold. Newer jobs (e.g. `apps/ibge/**`, `anac_aerodromo` bronze) follow this; some
older silver/gold jobs still hardcode `s3a://...` literals — prefer `table_location` in new
and edited code.

### Job anatomy (the pattern every job follows)

1. Resolve `ORIGEM`/`DESTINO` (via `table_location`, or hardcoded in older jobs).
2. `main()` builds the session, sets `setLogLevel("ERROR")`, reads input.
   - Plain jobs: `SparkSession.builder.appName(...).getOrCreate()`.
   - **Any job touching geometry must use Sedona**:
     `SedonaContext.create(SedonaContext.builder().appName(...).getOrCreate())`, or the
     `st_*` functions are not registered.
3. Compose transforms — chain `.transform(fn)` with helpers from `utils/`; a `select`
   declares the output schema contract.
4. Write to the layer's location with `.mode("overwrite")`; geometry outputs use
   `.format("geoparquet")` and are written in **EPSG:3857**; read back and `print` the count.

### Layer conventions

- **bronze**: read raw CSV with an **explicit schema** (never `inferSchema`), `sep=";"`,
  standardize column names to `snake_case` via `.alias()` in the `select`, cast BR-formatted
  strings to proper types, filter invalid keys.
- **silver**: join bronze tables (lookups, geometry construction, derived metrics).
- **gold**: aggregations / rankings for consumption.

## Code style — read `CODING_STANDARDS.md` before writing PySpark

Full rules (Palantir-derived) are in `CODING_STANDARDS.md`. The load-bearing ones:

- **Language split**: code/identifiers/column-names in **English**; docstrings and comments
  in **Portuguese**.
- **No UDFs** and **no `F.expr`** — always use native `pyspark.sql.functions`; reuse a
  `utils/` helper before writing a new generic transform (new generic ones belong in
  `utils/`, not in a job).
- Model each step as a pure `DataFrame -> DataFrame` / `Column -> Column` function; chain
  with `.transform(...)`. Treat `select` as the schema contract; cast inside `select`, not
  with `withColumn`.
- **Absence is never zero**: fill empty columns with `F.lit(None)` (never `""`/`"NA"`); do
  not coalesce nulls to 0 before aggregating unless the contract says so.
- Reference columns with `F.col("name")` (not `df.name`); explicit `how=` on every join and
  no `right` joins; never `.dropDuplicates()`/`.distinct()` to paper over unexpected dupes.
- Window functions **always** get an explicit frame (`rowsBetween`/`rangeBetween`); avoid
  empty `partitionBy()` — use `.agg` instead.
- **Geospatial**: prefer Sedona's Python API over SQL strings; latitude/longitude are always
  `double`; measure geodesic distance (`ST_DistanceSpheroid`, WGS84) on the **original**
  coordinates **before** projecting — never round-trip through 3857 just to measure.
- Imports: `from pyspark.sql import types as T, functions as F`. Files ≤ ~250 lines,
  functions ≤ ~70 lines.
