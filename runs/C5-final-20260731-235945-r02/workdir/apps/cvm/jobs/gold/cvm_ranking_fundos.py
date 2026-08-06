from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("cvm", "cota_serie", "silver")
DESTINO = table_location("cvm", "ranking_fundos", "gold")


def main():
    """Rank investment funds by their most recent net worth."""
    spark = SparkSession.builder.appName("cvm-ranking-fundos-gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    serie = spark.read.parquet(ORIGEM)

    latest = dt.dedupe_keep_latest(serie, keys=["cnpj_fundo"], order_col="data")

    ranked = dt.rank_within_group(
        latest,
        partition_cols=[],
        order_col="patrimonio_liquido",
        out_col="rank_patrimonio",
    )

    result = ranked.select(
        "cnpj_fundo",
        "patrimonio_liquido",
        "cotistas",
        "rank_patrimonio",
    ).orderBy(F.col("rank_patrimonio").asc())

    result.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> gold cvm_ranking_fundos: {n} fundos em {DESTINO}")
    result.show(10, truncate=False)
    spark.stop()


main()
