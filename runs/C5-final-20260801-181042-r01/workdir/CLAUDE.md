# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PySpark + Apache Sedona geospatial ETL pipelines over Brazilian public-data sources
(ANA, ANAC, BCB, CAGED, CVM, DATASUS, DNIT, EDUCACAO, IBGE, INMET, SNIS, TSE — one
per directory under `apps/`). Data lands in an S3-compatible object store (MinIO) as a
**medallion data lake**: `raw → bronze → silver → gold`. Each layer is a set of
standalone `spark-submit` jobs. There is no orchestrator in-repo; jobs are run one at a
time.

## Language convention (from CODING_STANDARDS.md)

- **Code, function/variable names, and column names: English.**
- **Docstrings and comments: Portuguese.**
Match this when adding or editing code.

## Layout

```
apps/<app>/
  jobs/bronze/  jobs/silver/  jobs/gold/   # one .py per output table, each with a main()
  utils/column_transforms.py               # app-specific Column -> Column helpers
  metadata.toml                            # declares every table, its layers, key, and per-layer schema
utils/
  metadata.py            # table_location(app, table, level) -> s3a:// path
  column_transforms.py   # shared Column -> Column functions
  dataframe_transforms.py# shared DataFrame -> DataFrame functions (incl. Sedona geo + window helpers)
conf/spark-defaults.conf # S3A wiring (read via SPARK_CONF_DIR)
docker/resources/        # image build scripts (GDAL compile, venv + Spark jars, entrypoint)
```

## The three cross-cutting mechanisms

**1. `metadata.toml` is the source of truth for paths and schemas.** Jobs must not build
S3 paths by hand — call `utils.metadata.table_location(app, table, level)`, which reads
the app's `metadata.toml` and returns `s3a://{db}/{app}/{name}/{level}`. It raises if the
table isn't declared or the layer wasn't ingested (fail-fast, not an empty read). A new
table or layer must be declared in `metadata.toml` first (with its per-layer schema).
Note: some older gold jobs still hardcode `s3a://...` strings — prefer `table_location`
for new code.

**2. Transforms are pure functions, reused from `utils/`.** Model each step as
`DataFrame -> DataFrame` or `Column -> Column`, no side effects, and chain them with
`DataFrame.transform(...)`. Before writing a generic transform, check `utils/` — a large
library already exists (text/coordinate cleaning, null handling, window/ranking/moving-
average, H3, Sedona geometry). Generic helpers belong in `utils/`; domain-specific ones
in `apps/<app>/utils/`. Import aliases are fixed: `from pyspark.sql import types as T,
functions as F`; shared modules as `ct` / `dt`.

**3. Layer responsibilities.**
- **bronze**: read `raw` (CSV, explicit schema, no `inferSchema`), lowercase/`snake_case`
  column names via `.alias()` in the `select`, apply cleaning utils, filter invalid keys,
  write Parquet. lat/long become `double` here.
- **silver**: geospatial + consolidation. Uses `SedonaContext`, builds point/polygon
  geometries, writes **geoparquet**, geometry stored in **EPSG:3857**.
- **gold**: aggregations, rankings, and H3 hexbin density tables.

## Geospatial rules (Sedona) — easy to get subtly wrong

- Distances in meters use **geodesic** distance (`ST_DistanceSpheroid`, WGS84 ellipsoid),
  never euclidean-in-degrees.
- **Measure distance on the ORIGINAL WGS84 point, BEFORE reprojecting to 3857.** The
  `4326→3857→4326` round-trip loses precision. Storing geometry in 3857 is an output
  convention only — compute first, project last. See `apps/ana/jobs/silver/ana_estacao_geo.py`.
- Prefer the Sedona Python API (`stf.*` / `stc.*`) over hand-written SQL, same as the
  `F.expr` rule below.

CODING_STANDARDS.md is the full style guide (Palantir PySpark style, adapted). Key rules
beyond the above: prefer native `F.<fn>` over `F.expr`; use `F.col("name")` not `df.name`;
`select` is the schema contract (one function per column, cast inside `select`); absence is
`F.lit(None)`, never `""`/`0` (and nulls are not zero in aggregations); always give window
functions an explicit frame (`rowsBetween`/`rangeBetween`); always state `how` on joins and
never mask duplicates with `dropDuplicates`/`distinct`; no UDFs. Read it before non-trivial
PySpark changes.

## Running jobs

Everything runs inside the Docker image (Fedora 39, Java 11, Python 3.11, GDAL 3.8.1 built
from source, Spark 3.5.4 + Sedona 1.9.0 jars baked into the venv). The repo is mounted at
`/workspace/spark-project`; `PYTHONPATH` is the project root so `from utils... import` and
`from apps.<app>... import` resolve.

```bash
docker compose build                       # build the image (GDAL compile ~10 min, cached)
docker network create external-net         # one-time: compose expects this external network
docker compose up -d                       # start the long-lived container (needs an S3 service on external-net)

# run a single job:
docker compose exec spark spark-submit apps/<app>/jobs/<layer>/<job>.py
```

Config: copy `.env.example` to `.env` (S3 endpoint/bucket + `AWS_*` credentials, consumed by
the S3A `EnvironmentVariableCredentialsProvider`). Spark UI on host `:14040`, Jupyter on
`:18888`.

## Tests

There is no test suite in the repo (and `validate_setup.py` referenced by the container
banner is not present). CODING_STANDARDS.md asks that changes be validated by running the
relevant job and checking the output data — each job prints a record count after writing.
</content>
</invoke>
