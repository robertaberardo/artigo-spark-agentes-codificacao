from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from utils import dataframe_transforms as dt

ORIGEM = "s3a://datalake/ana/ana_medicao_consolidada/silver"
DESTINO = "s3a://datalake/ana/ana_ranking_estacoes/gold"


def main():
    """Ranking das estações por vazão média dentro de cada UF."""
    spark = SparkSession.builder.appName("ana-ranking-estacoes-gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    medicoes = spark.read.parquet(ORIGEM)

    estacoes = medicoes.groupBy("codigo_estacao", "uf").agg(
        F.round(F.avg("vazao_m3s"), 2).alias("vazao_media")
    )

    result = dt.rank_within_group(
        estacoes, partition_cols=["uf"], order_col="vazao_media", out_col="rank"
    ).orderBy("uf", F.col("rank").asc())

    result.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> gold ana_ranking_estacoes: {n} estações em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
