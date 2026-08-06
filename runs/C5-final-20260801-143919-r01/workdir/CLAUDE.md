# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PySpark + Apache Sedona geospatial data pipelines over Brazilian public-data sources, organized as a **medallion lake** (raw → bronze → silver → gold) on S3A-compatible object storage (MinIO). Runs inside a Fedora/Java 11 Docker container with Spark 3.5.4, Sedona 1.9.0, and GDAL 3.8.1.

## Language convention (strict)

- **Code, function/variable names, and column names: English.**
- **Docstrings and comments: Portuguese.**
Match this when adding or editing code — it is the established convention throughout the repo.

## Layout

- `apps/<source>/` — one app per data source (`ana`, `anac`, `bcb`, `caged`, `cvm`, `datasus`, `dnit`, `educacao`, `ibge`, `inmet`, `snis`, `tse`). Each contains:
  - `jobs/{bronze,silver,gold}/` — one spark-submit script per output table, each with a `main()`.
  - `utils/column_transforms.py` — **domain-specific** column transforms for that source only.
  - `metadata.toml` — declares every table, its layers, key, and per-layer schema.
- `utils/` — **shared, source-agnostic** transforms reused across all apps:
  - `column_transforms.py` — functions that take and return a `Column`.
  - `dataframe_transforms.py` — functions that take and return a `DataFrame` (joins, windows, H3/Sedona geo, flatten, union).
  - `metadata.py` — `table_location(app, table, level)` resolves the S3A path from `metadata.toml`.
- `conf/spark-defaults.conf` — S3A plumbing (read via `SPARK_CONF_DIR`); credentials come from env, never code.
- `docker/resources/` — image build scripts (`install-gdal.sh`, `setup-env.sh`, `start.sh`).

## The medallion flow

Each job reads from one layer and writes to the next, always ending by **persisting** to the lake:

- **bronze** — reads raw CSV with an **explicit `RAW_SCHEMA`** (never `inferSchema`), applies column transforms in a single `select` (lowercasing/aliasing column names here), filters, writes **parquet**. See `apps/ana/jobs/bronze/ana_estacao.py`.
- **silver** — consolidation, joins, and geospatial. Geo jobs create a `SedonaContext`, build geometry, and write **geoparquet**. See `apps/ana/jobs/silver/ana_estacao_geo.py` (geo) and `ana_medicao_consolidada.py` (join + window).
- **gold** — aggregations, rankings, and H3 hexbins. See `apps/ana/jobs/gold/ana_ranking_estacoes.py` and `ana_estacoes_hexbin.py`.

Standard job shape: `SparkSession`/`SedonaContext` builder → `setLogLevel("ERROR")` → read → chained `.transform(...)` steps → `write.mode("overwrite")` → read back and `print` a count → `spark.stop()`.

## Table locations: use metadata, not hardcoded paths

Resolve every path with `table_location(app, table, level)` — it reads `metadata.toml`, builds `s3a://{db}/{app}/{name}/{level}`, and **fails early** (`KeyError`/`ValueError`) if the table or layer isn't declared. New tables and layers must be declared in the app's `metadata.toml` (including the per-layer schema) *before* a job can reference them. A minority of older gold jobs still hardcode `s3a://...` strings — prefer `table_location` for anything new and when touching those.

## Sedona / geospatial rules

- Geo jobs must run under `SedonaContext.create(...)`; the shared geo transforms register nothing on their own.
- Geometry stored in the lake is **EPSG:3857** (Web Mercator); reproject with `dt.to_web_mercator` / `dt.reproject_geometry` as the *output* convention.
- Measure geodesic distances (`ST_DistanceSpheroid`) on the **original WGS84 coordinates BEFORE projecting** — never round-trip 4326→3857→4326 to measure (it loses precision at Brazilian latitudes). Compute distance first, project the geometry afterward.
- Lat/long are always `double`; H3 hexbins use `dt.attach_h3_index` + `dt.aggregate_by_h3`.

## Reusing transforms

Before writing a generic transform, **check `utils/` first** and reuse it (`ct` for column-level, `dt` for DataFrame-level). New *generic* transforms belong in `utils/`; source-specific ones go in the app's own `utils/column_transforms.py`. Import aliases are fixed: `from pyspark.sql import types as T, functions as F`, `from utils import column_transforms as ct`, `from utils import dataframe_transforms as dt`.

`CODING_STANDARDS.md` is the full PySpark style guide (based on the Palantir guide) and is authoritative — read it before non-trivial changes. Key rules: no UDFs; prefer native `F.*` over `F.expr`; `F.col("name")` not attribute access; `select` as an explicit schema contract; `F.lit(None)` for missing values (absence ≠ zero); explicit `how` on joins and no `dropDuplicates`/`distinct` to mask fan-out (use `dt.join_no_fanout`); always give window functions an explicit frame.

## Running

The container stays alive (`tail -f /dev/null`); run jobs into it with `spark-submit`. The compose file attaches to an **external** Docker network named `external-net` (create it once with `docker network create external-net`) where the S3 endpoint `http://s3:9000` must be reachable.

```bash
# create the shared network once (if missing)
docker network create external-net

# build the image (GDAL compile is ~10 min, cached across dep changes)
docker compose build

# start the long-running container
docker compose up -d

# run a single job (source-relative paths, PYTHONPATH is the project root)
docker compose exec spark spark-submit apps/ana/jobs/bronze/ana_estacao.py
```

Config: copy `.env.example` to `.env` (S3 endpoint/bucket, `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, host ports for Spark UI `14040` and Jupyter `18888`). `S3_BUCKET` overrides the `db` from metadata.

## Testing

There is no automated test suite in the repo. Validate changes by running the affected job end-to-end (each prints its output row count) and by checking that the written data matches the schema declared in `metadata.toml`.
