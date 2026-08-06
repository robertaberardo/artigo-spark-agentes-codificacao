from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("inmet", "medicao_diaria", "silver")
DESTINO = table_location("inmet", "ranking_chuva", "gold")


def main():
    """Ranqueia as estações pela chuva total acumulada no período."""
    spark = SparkSession.builder.appName("inmet-ranking-chuva-gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    diaria = spark.read.parquet(ORIGEM)

    totais = diaria.groupBy(diaria["codigo_estacao"]).agg(
        F.round(F.sum(diaria["precip_total"]), 2).alias("precip_total")
    )

    result = dt.rank_within_group(
        totais, partition_cols=[], order_col="precip_total",
        out_col="rank_chuva",
    ).orderBy(F.col("rank_chuva").asc())

    result.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> gold inmet_ranking_chuva: {n} estações em {DESTINO}")
    result.show(10, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
