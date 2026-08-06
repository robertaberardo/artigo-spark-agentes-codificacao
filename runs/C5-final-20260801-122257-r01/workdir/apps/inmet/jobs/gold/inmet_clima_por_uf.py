from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

MEDICAO = table_location("inmet", "medicao_diaria", "silver")
ESTACAO = table_location("inmet", "estacao_geo", "silver")
DESTINO = table_location("inmet", "clima_por_uf", "gold")


def main():
    """Climate averages per state (UF), joining daily measurements to stations."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("inmet-clima-por-uf-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    medicao = spark.read.parquet(MEDICAO)
    estacao = spark.read.format("geoparquet").load(ESTACAO).select(
        F.col("codigo_wmo").alias("codigo_estacao"), F.col("uf")
    )

    joined = dt.join_lookup(medicao, estacao, "codigo_estacao")

    gold = (
        joined.groupBy("uf")
        .agg(
            F.round(F.avg("temp_media"), 2).alias("avg_temp_c"),
            F.round(F.avg("precip_total"), 2).alias("avg_precip_mm"),
            F.round(F.avg("umidade_media"), 2).alias("umidade_media"),
            F.countDistinct("codigo_estacao").cast("long").alias("station_count"),
        )
        .orderBy(F.col("avg_precip_mm").desc())
    )

    gold.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> gold inmet_clima_por_uf: {n} UFs em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
