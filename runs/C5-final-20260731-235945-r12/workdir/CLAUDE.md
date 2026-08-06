# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A collection of PySpark batch pipelines that ingest and transform Brazilian public-data sources (ANAC, DATASUS, IBGE, DNIT, TSE, BCB, etc.) into a medallion-layered S3-compatible data lake. Geospatial work uses Apache Sedona; heavy geometry/CRS work relies on a GDAL/PROJ build baked into the image.

Language convention (see `CODING_STANDARDS.md`): **code, identifiers, and column names in English; docstrings and comments in Portuguese.** Match this in new code.

## Architecture

**Per-source apps.** Each `apps/<app>/` is one data source and is self-contained:
- `metadata.toml` — the single source of truth for that source's tables: their name, which `levels` they exist at, the natural `key`, and a per-level column `schema` (name/type/description).
- `jobs/{bronze,silver,gold}/<table>.py` — one `spark-submit`-able script per table transformation. Each defines `main()`, builds its own Spark/Sedona session, reads inputs, transforms, and **persists** the result before exiting (`if __name__ == "__main__": main()`).
- `utils/column_transforms.py` — column transforms specific to that domain (e.g. `anac.utils.is_valid_oaci`).

**Shared library at the repo root** (`utils/`, on `PYTHONPATH`):
- `utils/metadata.py` — `load_metadata(app)` and `table_location(app, table, level)`. The latter returns the S3A path `s3a://{bucket}/{app}/{table_name}/{level}`, validating the table and level exist (raises `KeyError`/`ValueError` — fail early instead of reading an empty prefix). Prefer this over hand-writing paths; a minority of older jobs hardcode `s3a://...` literals.
- `utils/column_transforms.py` — reusable `Column -> Column` functions (nulls, text, BR number/date/money parsing, arrays/maps, Sedona geometry helpers, window helpers).
- `utils/dataframe_transforms.py` — reusable `DataFrame -> DataFrame` functions (joins/lookups, coordinate validation, H3 gridding, CRS reprojection, ranking/window ops, JSON flattening).

**Medallion flow.** `raw` (external CSV/source files) → `bronze` (typed, cleaned, snake_case columns; latitude/longitude cast to `double`) → `silver` (joined/consolidated, often georeferenced) → `gold` (aggregations, rankings). Non-geo output is Parquet; geospatial output is **GeoParquet** (`.format("geoparquet")`), with geometry stored in EPSG:3857.

**Storage & config.** All I/O goes to S3A (MinIO/S3). S3A connection wiring lives in `conf/spark-defaults.conf` (loaded via `SPARK_CONF_DIR`); the bucket and credentials come from env vars (`S3_BUCKET`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`). `table_location` honors `S3_BUCKET` as an override for the metadata's `db`. Job code contains no connection config.

## Conventions that are easy to get wrong

`CODING_STANDARDS.md` is the authority (adapted Palantir PySpark style guide) — read it before writing transforms. The highest-leverage rules:
- **Before writing a generic transform, check `utils/` — it is large.** New generic transforms belong in `utils/`, not in a job body. Domain-specific ones go in the app's `utils/column_transforms.py`.
- **No UDFs.** Use native `pyspark.sql.functions`. Prefer `F.something(...)` over `F.expr("...")`.
- Model each step as a pure `DataFrame -> DataFrame` / `Column -> Column` function and chain with `.transform(...)`.
- **Absence is never zero.** Fill empties with `F.lit(None)`, never `""`/`"NA"`; don't coalesce nulls to `0` before aggregating unless the contract says so.
- Explicit `schema` on read (never `inferSchema`); explicit `how` on every join; latitude/longitude always `double`.
- Geodesic distance (`ST_DistanceSpheroid` over WGS84) for meters — measured on the **original** WGS84 coordinates *before* projecting to 3857; do not round-trip through a projection just to measure.
- Prefer the Sedona Python API (`st_functions`/`st_constructors`) over hand-built SQL, same as the `F.expr` rule.
- New tables/levels must be declared in the app's `metadata.toml` (schema per level) before a job reads or writes them.

## Environment & commands

Everything runs inside the Docker image (Fedora + Java 11 + Python 3.11 + Spark 3.5.4 + Sedona 1.9.0 + GDAL 3.8.1). The repo is mounted at `/workspace/spark-project`; `utils/` imports work because `PYTHONPATH` points there.

```bash
# docker-compose expects a pre-existing external Docker network named 'external-net'
# (it attaches to an external S3/MinIO service reachable at http://s3:9000):
docker network create external-net   # once, if missing

cp .env.example .env                  # S3 endpoint/bucket/credentials + UI ports
docker compose build                  # GDAL compile is ~10 min; layer-cached
docker compose up -d                  # starts the 'spark' container (idles via tail -f)

# Run a single job (this is the primary dev loop — there is no orchestrator):
docker compose exec spark spark-submit apps/<app>/jobs/<layer>/<table>.py
```

Ports: Spark UI on `${SPARK_UI_PORT:-14040}` → 4040; Jupyter on `${JUPYTER_PORT:-18888}` → 8888.

There is no automated test suite. Validate a change by running the affected job and checking the printed record count / `.show()` output against the expected result (each job prints `>> <layer> <table>: N registros em <path>`). `spark-defaults.conf`, the Sedona/S3A jars, and GDAL are provisioned at image-build time (`docker/resources/setup-env.sh`), not at job runtime.
