from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils.metadata import table_location

ORIGEM = table_location("inmet", "medicao", "bronze")
DESTINO = table_location("inmet", "medicao_diaria", "silver")


def main():
    """Agrega as medições horárias para o grão diário por estação."""
    spark = SparkSession.builder.appName("inmet-medicao-diaria-silver").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    medicao = spark.read.parquet(ORIGEM)

    medicao = medicao.withColumn("data", F.to_date(F.col("data_hora")))

    diaria = medicao.groupBy("codigo_estacao", "data").agg(
        F.round(F.avg("temp_c"), 2).alias("temp_media"),
        F.round(F.avg("umidade_pct"), 2).alias("umidade_media"),
        F.round(F.sum("precip_mm"), 2).alias("precip_total"),
        F.round(F.avg("vento_ms"), 2).alias("vento_medio"),
    )

    diaria = diaria.orderBy("codigo_estacao", "data")

    diaria.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> silver inmet_medicao_diaria: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
