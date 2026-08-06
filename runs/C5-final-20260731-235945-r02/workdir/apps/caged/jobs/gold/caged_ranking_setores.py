from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("caged", "movimentacao_consolidada", "silver")
DESTINO = table_location("caged", "ranking_setores", "gold")


def main():
    """Rank economic sectors (CNAE) by number of labor movements."""
    spark = SparkSession.builder.appName("caged-ranking-setores-gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    movimentacoes = spark.read.parquet(ORIGEM)

    setores = movimentacoes.groupBy("cnae").agg(
        F.count(F.lit(1)).cast("long").alias("total_movimentacoes")
    )

    result = dt.rank_within_group(
        setores, partition_cols=[], order_col="total_movimentacoes", out_col="rank",
    ).orderBy(F.col("rank").asc())

    result.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> gold caged_ranking_setores: {n} setores em {DESTINO}")
    result.show(10, truncate=False)
    spark.stop()


main()
