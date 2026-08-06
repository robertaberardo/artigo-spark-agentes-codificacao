from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from utils import dataframe_transforms as dt

ORIGEM = "s3a://datalake/anac/anac_voo_consolidado/silver"
DESTINO = "s3a://datalake/anac/anac_ranking_rotas/gold"


def main():
    """Rank routes by total passengers over the consolidated flights table."""
    spark = SparkSession.builder.appName("anac-ranking-rotas-gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    voos = spark.read.parquet(ORIGEM)

    rotas = (
        voos.withColumn(
            "rota", F.concat_ws("-", F.col("origem_oaci"), F.col("destino_oaci"))
        )
        .groupBy("rota", "origem_oaci", "destino_oaci")
        .agg(
            F.count(F.lit(1)).alias("voos"),
            F.sum("passageiros").cast("long").alias("total_passageiros"),
        )
    )

    result = dt.rank_within_group(
        rotas, partition_cols=[], order_col="total_passageiros",
        out_col="rank_passageiros",
    ).orderBy(F.col("rank_passageiros").asc())

    result.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> gold anac_ranking_rotas: {n} rotas em {DESTINO}")
    result.show(10, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
