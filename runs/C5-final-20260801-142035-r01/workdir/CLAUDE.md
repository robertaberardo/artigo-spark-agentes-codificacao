# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A collection of PySpark batch pipelines that ingest and transform Brazilian public-data sources into a medallion data lake (raw → bronze → silver → gold). Stack: **PySpark 3.5.4 + Apache Sedona 1.9.0 (geospatial) + GDAL**, Python 3.11, running against an S3-compatible object store (MinIO) via the S3A connector. This is described as a *validation environment* — the same jobs are meant to run unchanged on a production cluster.

**Read `CODING_STANDARDS.md` before writing or editing any PySpark code.** It is the authoritative style guide (a Palantir-derived PySpark guide adapted to this repo) and its rules are enforced expectations, not suggestions.

## Language conventions (from CODING_STANDARDS.md)

- **Code, function/variable names, and column names: English.**
- **Docstrings and comments: Portuguese.**
- Transformed column names (bronze onward) are `snake_case`, lowercased in the bronze `select` via `.alias(...)`. Raw reads reference the source's original column casing.

## Architecture

### App layout — every source is a self-contained "app"
`apps/<app>/` (12 apps: `ana`, `anac`, `bcb`, `caged`, `cvm`, `datasus`, `dnit`, `educacao`, `ibge`, `inmet`, `snis`, `tse`). Each app has:

- `metadata.toml` — the single source of truth for table locations and per-layer schemas (see below).
- `jobs/{bronze,silver,gold}/<name>.py` — one runnable Spark job per file, each with a `main()` that builds a session, transforms, and **persists** the result.
- `utils/column_transforms.py` — domain-specific `Column -> Column` helpers for that app (e.g. `ana.is_valid_station_code`).

### Medallion layers
- **bronze**: reads the `raw` layer (CSV from S3A, explicit schema, `sep=";"` common for BR data), standardizes types/casing, writes Parquet. Latitude/longitude become `double` here.
- **silver**: joins/consolidates and adds geometry. Geospatial jobs use `SedonaContext`, write `geoparquet`, store geometry as `EPSG:3857`.
- **gold**: aggregations, rankings, and H3 hexbin densities for map-ready outputs.

### Shared code — `utils/` (top-level, on `PYTHONPATH`)
Reuse before writing new generic transforms. Two modules split by return type:
- `utils/column_transforms.py` — `Column -> Column` (e.g. `normalize_text`, `br_decimal_to_double`, `parse_br_date`, `standardize_uf`, `to_coordinate`).
- `utils/dataframe_transforms.py` — `DataFrame -> DataFrame` (e.g. `filter_valid_coordinates`, `add_point_geometry`, `to_web_mercator`, `attach_h3_index`, `aggregate_by_h3`, `join_no_fanout`, `rank_within_group`, `add_moving_average`).
- `utils/metadata.py` — `load_metadata(app)` and `table_location(app, table, level)`.

Jobs chain these with `DataFrame.transform(...)` rather than reassigning per step. New *generic* transforms belong in `utils/`; app-specific ones in `apps/<app>/utils/`.

### Metadata drives all paths — never hardcode a table path
Jobs resolve locations through `table_location(app, table, level)` (from `utils.metadata`), which returns `s3a://{bucket}/{app}/{table_name}/{level}`. The bucket comes from `S3_BUCKET` (env) overriding `data_lake.db` in the TOML. Adding a table or layer means declaring it in `metadata.toml` **with its per-layer schema** — `table_location` raises if the table isn't declared or the requested level isn't ingested yet.

Typical job header:
```python
from utils import column_transforms as ct         # or dataframe_transforms as dt
from utils.metadata import table_location
ORIGEM  = table_location("ana", "estacao", "bronze")
DESTINO = table_location("ana", "estacao_geo", "silver")
```

### Geospatial specifics (Sedona)
- Prefer Sedona's Python API over hand-written SQL expressions.
- Compute geodesic distances (`ST_DistanceSpheroid`, WGS84) over the **original** lat/long **before** projecting to 3857 — never round-trip `4326→3857→4326` just to measure. Storing geometry in 3857 is an output convention only.
- Latitude/longitude are always `double`, never string.

## Running jobs

Everything runs inside the Docker container (Fedora 39, Java 11, venv at `/opt/venv`). The container needs an external Docker network `external-net` and an S3 service reachable at `s3:9000` (config in `conf/spark-defaults.conf`; credentials via `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`).

```bash
# Bring up the container (idle-loops via docker/resources/start.sh)
docker compose up -d --build

# Run a single job (working dir is the project root, so relative paths work)
docker compose exec spark spark-submit apps/ana/jobs/bronze/ana_bacia.py

# Spark UI → localhost:14040 ; Jupyter → localhost:18888 (ports from .env)
```

Jobs must be run in dependency order (raw must exist before bronze, bronze before silver, etc.). Copy `.env.example` to `.env` first.

## Testing

There is no automated test suite in the repo. Per CODING_STANDARDS.md, validation is done by running the job and confirming the output data (row counts, schema) is as expected — each job prints a `>>` summary line after writing. Model transforms as pure, isolatable functions so they can be checked independently.

## Key style rules to respect (see CODING_STANDARDS.md for the full list)
- No UDFs; prefer native `pyspark.sql.functions`. Prefer `F.some_fn(...)` over `F.expr("...")`.
- Use `F.lit(None)` for missing values — never `""` or `0`. Absence is not zero; don't zero-fill before aggregating.
- `select` is the schema contract: cast inside `select`, keep it explicit (no `select("*")` + `drop`).
- Always specify `how` on joins; avoid `right` joins; don't paper over duplicates with `.distinct()`/`.dropDuplicates()`.
- Window functions must declare an explicit frame (`rowsBetween`/`rangeBetween`); avoid empty `partitionBy()`.
- Keep files ≤ ~250 lines, functions ≤ ~70 lines, chains ≤ 5 expressions; extract complex boolean logic into named variables.
- Standard imports: `from pyspark.sql import types as T, functions as F`.
