# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PySpark + Apache Sedona + GDAL geospatial pipelines over Brazilian public-data
sources (ANA, ANAC, BCB, CAGED, CVM, DATASUS, DNIT, educação, IBGE, INMET, SNIS,
TSE). It is a **validation environment**: sources are self-contained under
`apps/` and mounted into a container at runtime — there is no repo layer above
`spark-project/`.

**Language convention (from `CODING_STANDARDS.md`):** code, function/variable
names, and column names are in **English**; docstrings and comments are in
**Portuguese**. Match this when adding code.

## Architecture

### Medallion layers (raw → bronze → silver → gold)

Every app's jobs live under `apps/<app>/jobs/<layer>/`. A job is a standalone
`spark-submit` script with a `main()` that reads one layer and writes the next.

- **bronze** — read raw CSV with an **explicit schema** (never `inferSchema`),
  rename to `snake_case` lowercase via `.alias()`, cast types, drop invalid
  rows. Written as **parquet**.
- **silver** — georeferencing (Sedona points/polygons), cross-table joins /
  consolidation, window features. Geo tables are written as **geoparquet**.
- **gold** — aggregations, rankings, and H3 hexbin density grids for analytics.

Data is stored in an S3-compatible object store (MinIO) via Spark's S3A
connector. Canonical path: `s3a://{bucket}/{app}/{table_name}/{level}`.

### `metadata.toml` is the schema contract

Each app has `apps/<app>/metadata.toml` declaring every table: its physical
`name`, the `levels` it exists in, its `key`, and a **per-level schema**
(column name + type + description). This is the source of truth for both table
locations and schemas.

- **Jobs never hand-build S3 paths.** Resolve them through
  `utils.metadata.table_location(app, table, level)`, which reads `metadata.toml`
  and returns the S3A path. It raises `KeyError` for an undeclared table and
  `ValueError` for a level that hasn't been ingested (fail-fast, not an empty
  read). The bucket comes from `metadata.data_lake.db`, overridable by
  `S3_BUCKET`. (A few older gold jobs still hardcode `s3a://…` strings — do not
  copy that; use `table_location`.)
- New tables/layers must be **declared in `metadata.toml` first**, including the
  per-layer schema.

### Transformation library (`utils/`) — reuse before writing

Transformations are modeled as pure, testable functions and chained with
`DataFrame.transform(...)`. Two shared modules, plus per-app extensions:

- `utils/column_transforms.py` — `Column -> Column` (parsing BR decimals/dates/
  money, null handling, WKT/coordinate helpers, keys, bucketize, etc.).
- `utils/dataframe_transforms.py` — `DataFrame -> DataFrame` (joins without
  fan-out, H3 gridding, reprojection, window features, unions, flatten, etc.).
- `apps/<app>/utils/column_transforms.py` — domain-specific column functions
  (e.g. `ana`'s 8-digit station-code validator).

**Before writing a new generic transform, check `utils/` for an existing one.**
New *generic* transforms belong in `utils/`, not in a job body; app-specific
ones go in `apps/<app>/utils/`. Sedona-backed helpers require an active
`SedonaContext`.

## Coding standards

`CODING_STANDARDS.md` (Palantir PySpark style, adapted) is authoritative — read
it before non-trivial work. Load-bearing rules:

- **No UDFs.** Rewrite with native `pyspark.sql.functions`; prefer `F.<fn>(...)`
  over `F.expr("…")`, and Sedona's Python API over hand-written spatial SQL.
- Absence is **`F.lit(None)`**, never `""`/`"NA"`/`0`; nulls are not zero in
  aggregations.
- Joins: always state `how` explicitly; no `right` joins; never use
  `dropDuplicates`/`distinct` to paper over unexpected fan-out (investigate the
  cause — `dataframe_transforms.join_no_fanout` enforces this).
- Window functions: always specify an explicit frame (`rowsBetween`/
  `rangeBetween`); avoid empty `partitionBy()`.
- `select` is the schema contract; cast inside `select`, keep it simple.
- **Geospatial distance:** use geodesic `ST_DistanceSpheroid` on the **original
  WGS84** coordinates *before* projecting. Never round-trip 4326→3857→4326 just
  to measure — it loses precision at Brazilian latitudes. Projecting geometry to
  EPSG:3857 is an *output* convention only.
- lat/long are always `double`. Files ≲250 lines, functions ≲70 lines.
- Import aliases: `from pyspark.sql import types as T, functions as F`.

## Running jobs

Everything runs inside the Docker container (Fedora 39 + Java 11 + Python 3.11 +
GDAL 3.8.1 + PySpark 3.5.4 + Sedona 1.9.0). Spark jars (Sedona, hadoop-aws, AWS
SDK) and GDAL are baked into the image; Spark reads `conf/spark-defaults.conf`
(S3A plumbing) via `SPARK_CONF_DIR`, and `PYTHONPATH` is the project root so
`from utils…` / `from apps…` imports resolve.

```bash
# One-time: the compose file joins an EXTERNAL docker network `external-net`
docker network create external-net        # if it doesn't already exist

docker compose up -d --build               # build image + start the `spark` service
# The container just stays alive (start.sh runs `tail -f /dev/null`); run jobs into it:
docker compose exec spark spark-submit apps/ana/jobs/bronze/ana_estacao.py

# Respect layer order — silver reads bronze, gold reads silver.
```

Config comes from `.env` (copy `.env.example`): `S3_ENDPOINT`, `S3_BUCKET`,
`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (read by S3A via
`EnvironmentVariableCredentialsProvider`), plus `SPARK_UI_PORT` (host 14040 →
4040) and `JUPYTER_PORT` (host 18888 → 8888).

There is no automated test suite. Validate a change by running its job and
checking the printed record count / that the output looks right, per the last
rule in `CODING_STANDARDS.md`. (`start.sh` advertises `validate_setup.py` and an
`exploration/` Jupyter launcher; neither is present in the tree yet.)
