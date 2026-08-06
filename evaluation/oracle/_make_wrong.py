"""§5b — gera variantes da tabela-alvo com UM erro plantado cada, para provar
que o oráculo (`full_compare.py`) reprova no item CERTO.

Lê o gabarito e escreve em `<out>/<caso>/`:
  * zero_for_null   — `0.0` no lugar de NULL em `avg_dist_neighbors_km` (isoladas)
                      -> deve reprovar 1.6 (testa o conserto do nulo).
  * wrong_total     — `total_enrollment` deslocado -> deve reprovar 1.3.
  * isolated_nonzero— célula isolada com `n_federal_neighbors`=5 -> reprova 1.5
                      (coerência contagem⇄nulo).

Uso: spark-submit _make_wrong.py [--gab <dir>] [--out <dir>]
"""
import sys

from pyspark.sql import functions as F
from sedona.spark import SedonaContext


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


def main():
    gab = arg("--gab", "/data/gabaritos/educacao_escola_matricula_h3_grid")
    out = arg("--out", "/data/gabaritos/_wrong")
    spark = SedonaContext.create(
        SedonaContext.builder().appName("make-wrong").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    g = spark.read.format("geoparquet").load(gab)

    variants = {
        "zero_for_null": g.withColumn(
            "avg_dist_neighbors_km",
            F.coalesce(F.col("avg_dist_neighbors_km"), F.lit(0.0)),
        ),
        "wrong_total": g.withColumn(
            "total_enrollment", (F.col("total_enrollment") + F.lit(1)).cast("long")
        ),
        "isolated_nonzero": g.withColumn(
            "n_federal_neighbors",
            F.when(F.col("avg_dist_neighbors_km").isNull(), F.lit(5))
             .otherwise(F.col("n_federal_neighbors")).cast("long"),
        ),
    }
    for name, df in variants.items():
        dest = f"{out}/{name}"
        df.coalesce(1).write.mode("overwrite").format("geoparquet").save(dest)
        print(f">> variante '{name}' -> {dest}")
    spark.stop()


if __name__ == "__main__":
    main()
