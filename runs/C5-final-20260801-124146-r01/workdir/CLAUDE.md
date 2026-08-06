# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A collection of PySpark batch pipelines that ingest and transform Brazilian
public-data sources (ANAC, IBGE, DATASUS, TSE, BCB, CAGED, CVM, DNIT, INMET,
SNIS, ANA) into a medallion-layered data lake. Geospatial work uses Apache
Sedona + GDAL. The project describes itself as a *validation environment*: it is
a self-contained mirror of a production cluster's job code, meant to be run and
verified against a local S3-compatible object store.

`CODING_STANDARDS.md` is the authoritative style guide (adapted Palantir PySpark
guide) — **read it before writing or reviewing any job.** The points below are
the architecture that guide does not cover.

## Running jobs

Everything runs inside the `spark` container (Fedora 39, Java 11, Python 3.11
venv at `/opt/venv`, GDAL + Sedona/S3A jars baked in). The container `tail -f`s
to stay alive; you exec into it to submit jobs.

```bash
docker compose up -d --build          # first run builds the image (GDAL compile ~10 min)
docker compose exec spark spark-submit apps/<app>/jobs/<layer>/<job>.py
```

- Jobs are plain `spark-submit` scripts run from the project root (`WORKDIR`);
  relative module imports (`from utils... import`) rely on `PYTHONPATH` = project root.
- Spark UI: host port `14040` → container `4040`. Jupyter: `18888` → `8888`.
- The external Docker network `external-net` and the S3 endpoint `http://s3:9000`
  (service name `s3`) must exist — this project only brings the `spark` service.
- Config: copy `.env.example` → `.env`. S3 credentials reach Spark's S3A
  connector via the `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars
  (see `conf/spark-defaults.conf`); `S3_BUCKET` overrides the lake bucket.

There is no test suite and no lint config. Per `CODING_STANDARDS.md`,
verification is manual: run the job and confirm row counts / sampled output.
Every job's `main()` ends by re-reading its output and printing a count — that
print is the smoke test. `start.sh` references `validate_setup.py` and
`exploration/`, which do not exist yet.

## Layout and data flow

```
apps/<source>/
  metadata.toml            # table + per-layer schema declarations (the contract)
  jobs/{bronze,silver,gold}/<source>_<table>.py
  utils/column_transforms.py  # domain-specific Column->Column helpers
utils/                     # SHARED, cross-app transforms (see below)
conf/spark-defaults.conf   # S3A plumbing
```

Data lake is S3A (`s3a://{bucket}/{app}/{table}/{level}`), one prefix per
medallion layer. The layers form a strict dependency chain:

- **raw** — source CSVs, all columns `string`, original source column casing.
- **bronze** — one job per table. Reads raw with an **explicit schema** (never
  `inferSchema`), renames columns to `snake_case` in the `select`, applies
  `utils` column transforms, casts to real types, and writes Parquet. This is
  where cleaning happens.
- **silver** — joins/consolidates bronze tables; where geometry is built
  (`ST_Point` from lat/long, reprojected to EPSG:3857) and written as
  **geoparquet**. Reads bronze, writes silver.
- **gold** — aggregations, rankings, and H3 hexbin density layers for
  analytics. Reads silver (or bronze), writes Parquet / geoparquet.

A job reads exactly one layer and writes the next; it never skips layers.

## Table paths come from metadata, not string literals

`metadata.toml` per app is the schema/location contract. Resolve every path
through `utils.metadata.table_location(app, table, level)` — do **not** hand-build
`s3a://...` strings. `table_location` raises `KeyError` for undeclared tables and
`ValueError` for a layer not listed in that table's `levels`, so a typo fails
fast instead of reading an empty prefix. New tables/layers must be declared in
`metadata.toml` first (with `name`, `levels`, `key`, and the per-layer `schema`).

Note: ~15 of the ~94 jobs still hardcode `s3a://` literals (the older pattern).
`table_location` is the standard — follow it in new and edited jobs.

## The `utils/` transform library — reuse before you write

Two shared modules hold pure, chainable, UDF-free transforms. **Check here
before writing any generic transform**; new generic ones belong here, not inline
in a job (`CODING_STANDARDS.md` enforces this).

- `utils/column_transforms.py` — `Column -> Column` (and `Column`-factory)
  helpers: text/coordinate/BR-number parsing (`br_decimal_to_double`,
  `to_coordinate`, `money_br_to_double`), null-safety (`blank_to_null`,
  `safe_divide`, `coalesce_zero`), keys (`zero_pad_code`, `surrogate_key`),
  and Sedona geometry helpers (`fix_geometry`, `make_point`).
- `utils/dataframe_transforms.py` — `DataFrame -> DataFrame` helpers: joins
  (`join_lookup`, `join_no_fanout`), geospatial (`add_point_geometry`,
  `to_web_mercator`, `attach_h3_index`, `aggregate_by_h3`, `create_h3_grid_from_geom`),
  windowing (`rank_within_group`, `add_moving_average`), and `union_dataframes`.
  `BRAZIL_LAT/LON_MIN/MAX` bound coordinate validation to Brazil.

Apply them via `DataFrame.transform(...)`; for parameterized helpers use
`.transform(lambda d: dt.join_lookup(d, lookup, "key"))`. Import aliases are
fixed: `from utils import column_transforms as ct`, `... dataframe_transforms as dt`.

## Job skeleton (follow the existing shape)

```python
from pyspark.sql import functions as F
from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("<app>", "<table>", "<in_level>")
DESTINO = table_location("<app>", "<table>", "<out_level>")

def main():
    spark = SparkSession.builder.appName("<app>-<table>-<level>").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    # read -> chained .transform()/.select() -> write
    result.write.mode("overwrite").parquet(DESTINO)
    print(f">> <level> <table>: {spark.read.parquet(DESTINO).count()} ... em {DESTINO}")
    spark.stop()

if __name__ == "__main__":
    main()
```

- **Geospatial jobs** build the session with
  `SedonaContext.create(SedonaContext.builder()...getOrCreate())` instead of a
  bare `SparkSession` — the Sedona `st_*` functions are only registered on a
  Sedona context. Geometry outputs are written/read with
  `.format("geoparquet")`, not `.parquet(...)`.
- Distances in meters use **geodesic** `ST_DistanceSpheroid` over the original
  WGS84 coordinates, measured *before* projecting to 3857 (see the geospatial
  section of `CODING_STANDARDS.md` — the 4326→3857→4326 round-trip loses precision).

## Conventions worth internalizing

- **Language split:** code, identifiers, and column names in English; docstrings
  and comments in Portuguese.
- **Absence is `NULL`, never `0` or `""`** — and nulls are not zeroed before
  aggregation (`F.mean`/`F.sum` already ignore them). Only coalesce to `0` when
  the contract says absence *is* zero.
- **No UDFs**; prefer native `F.<fn>` over `F.expr(...)`; explicit `how=` on
  every join; explicit frames on every window; no `dropDuplicates`/`distinct`
  as a crutch for unexpected fan-out.
- lat/long are always `double` (never string) to preserve precision.
