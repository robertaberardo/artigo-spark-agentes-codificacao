from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from utils.metadata import table_location

ORIGEM = table_location("ana", "medicao_consolidada", "silver")
DESTINO = table_location("ana", "vazao_por_bacia", "gold")


def main():
    """Aggregate mean and maximum river flow by state and river."""
    spark = SparkSession.builder.appName("ana-vazao-por-bacia-gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    medicoes = spark.read.parquet(ORIGEM)

    gold = (
        medicoes.groupBy("uf", "rio")
        .agg(
            F.round(F.avg("vazao_m3s"), 2).alias("vazao_media"),
            F.round(F.max("vazao_m3s"), 2).alias("max_flow"),
            F.countDistinct("codigo_estacao").cast("long").alias("n_stations"),
        )
        .orderBy(F.col("vazao_media").desc())
    )

    gold.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> gold ana_vazao_por_bacia: {n} grupos em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
