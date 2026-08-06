# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PySpark + Apache Sedona geospatial data pipelines over Brazilian public-data
sources (validation environment). Each source is an **app** under `apps/`
(`ana`, `anac`, `bcb`, `caged`, `cvm`, `datasus`, `dnit`, `educacao`, `ibge`,
`inmet`, `snis`, `tse`). Jobs read/write a MinIO/S3 data lake via the S3A
connector (`s3a://...`) as Parquet / GeoParquet.

## Language convention (non-obvious, enforced repo-wide)

- **Code, function/variable names, and column names: English.**
- **Docstrings and comments: Portuguese.**

Match this when adding or editing code — do not "fix" Portuguese docstrings to
English or vice-versa.

## Read `CODING_STANDARDS.md` first

`CODING_STANDARDS.md` (PySpark style, adapted from the Palantir guide) is
authoritative for all PySpark in `apps/` and `utils/`. It is enforced, not
aspirational. The highest-leverage rules:

- Model each step as a pure `DataFrame -> DataFrame` or `Column -> Column`
  function; chain with `.transform(...)`. **Check `utils/` for an existing
  helper before writing a new generic transform** — new generic transforms
  belong in `utils/`, not in the job body.
- **No UDFs.** Rewrite with native `pyspark.sql.functions`.
- Prefer native `F.something(...)` over `F.expr("...")`; same for Sedona's
  Python API over hand-written spatial SQL. Comment *why* on the rare exception.
- Missing data is always `F.lit(None)` — never `""`, `"NA"`, or `0`. Absence
  is not zero (don't zero-fill before aggregating).
- Treat each `select` as the schema contract; do casts inside `select`, one
  `F.function` + optional `.alias()` per column.
- Always specify `how=` on joins; avoid `right` joins; never paper over
  duplicates with `.dropDuplicates()` / `.distinct()` — find the cause.
- Window functions: always an explicit frame (`rowsBetween`/`rangeBetween`);
  avoid empty `partitionBy()`.
- Geospatial distances in meters use **geodesic** distance
  (`ST_DistanceSpheroid` on WGS84), measured on the **original** lat/long
  *before* projecting. EPSG:3857 is an output convention for stored geometry
  only — never round-trip `4326→3857→4326` just to measure.
- Latitude/longitude are always `double`, never string. Use explicit schemas,
  never `inferSchema`.

## Architecture

### Medallion layers
Jobs are organized by layer under `apps/<app>/jobs/{bronze,silver,gold}/`:

- **bronze** — read raw CSV with an explicit `StructType`, standardize column
  names to lowercase `snake_case` via `select(...).alias(...)`, clean/cast with
  `utils` column transforms, write Parquet.
- **silver** — join/consolidate bronze tables, build geometries (Sedona),
  reproject to EPSG:3857, write GeoParquet.
- **gold** — aggregations, rankings, H3 hexbin density; write Parquet /
  GeoParquet.

Every job ends by **persisting** its result to the lake in the layer matching
the transformation, then reads it back to print a row count. Geospatial jobs
create the session via `SedonaContext.create(SedonaContext.builder()...)` and
use `.format("geoparquet")`; plain jobs use `SparkSession.builder`.

### Table locations come from `metadata.toml` — not hand-built paths
Each app has `apps/<app>/metadata.toml` declaring, per table: `name`, the
`levels` it exists in, the `key`, and a full schema per level. `utils/metadata.py`
resolves paths:

```python
from utils.metadata import table_location
ORIGEM  = table_location("ana", "estacao", "raw")      # -> s3a://{bucket}/ana/ana_estacao/raw
DESTINO = table_location("ana", "estacao", "bronze")
```

Path shape is `s3a://{db}/{app}/{name}/{level}`; `db` comes from
`data_lake.db` in the metadata but is **overridden by the `S3_BUCKET` env var**.
`table_location` raises if the table or requested level isn't declared — so
declare new tables/levels in `metadata.toml` (including the per-level schema)
before a job references them. Note: some jobs (e.g. `apps/anac/`) still hardcode
`s3a://...` string constants — prefer `table_location` for new/edited jobs.

### Shared vs. app-specific transforms
- `utils/column_transforms.py` — generic `Column -> Column` helpers (text/number
  normalization, BR decimals/dates/money, null handling, arrays/maps, Sedona
  geometry, window helpers like `forward_fill`).
- `utils/dataframe_transforms.py` — generic `DataFrame -> DataFrame` helpers
  (coordinate filtering, joins without silent fan-out, H3 grids, reprojection,
  ranking/moving-average windows, flatten nested/JSON).
- `apps/<app>/utils/column_transforms.py` — **domain-specific** column logic for
  that source only (e.g. `ana`'s `is_valid_station_code`).

Import aliases used throughout: `from utils import column_transforms as ct`,
`... dataframe_transforms as dt`, `from apps.<app>.utils import column_transforms
as <app>_ct`. Imports resolve because `PYTHONPATH` is the project root (set in
the Dockerfile).

## Running jobs

Everything runs inside the `spark` Docker service. The container installs a
Python 3.11 venv (`/opt/venv`), GDAL 3.8.1, and pre-downloads the Sedona 1.9.0 +
S3A jars, then stays alive (`tail -f /dev/null`) — you exec into it to run jobs.

```bash
docker compose build          # first time / after Dockerfile or pyproject.toml change
docker compose up -d          # start the container

# run a single job (paths are relative to the project root = WORKDIR):
docker compose exec spark spark-submit apps/ana/jobs/bronze/ana_estacao.py
docker compose exec spark spark-submit apps/ana/jobs/silver/ana_estacao_geo.py
```

- Bronze first, then silver, then gold — later layers read what earlier layers wrote.
- Requires a reachable S3-compatible store at `S3_ENDPOINT` (default
  `http://s3:9000`, a MinIO host named `s3`) and the external Docker network
  `external-net`. Copy `.env.example` to `.env` to set credentials/ports.
- Spark UI: `localhost:14040`. `conf/spark-defaults.conf` holds the S3A wiring;
  credentials come from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`.

There is no test suite or linter configured; "test the code" (per
`CODING_STANDARDS.md`) means running the job and verifying the output data.
Note: `docker/resources/start.sh` prints hints for `validate_setup.py` and
`exploration/start_jupyter.sh`, but those files are not present in the repo.

## File-size guidance (from the style guide)

Files ≤ ~250 lines, functions ≤ ~70 lines; chains ≤ 5 expressions; extract
named condition variables instead of packing >3 logical ops into one expression;
no commented-out code.
