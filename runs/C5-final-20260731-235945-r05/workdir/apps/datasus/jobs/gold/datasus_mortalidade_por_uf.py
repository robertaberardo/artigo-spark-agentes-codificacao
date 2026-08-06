from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ORIGEM = "s3a://datalake/datasus/datasus_obito_consolidado/silver"
DESTINO = "s3a://datalake/datasus/datasus_mortalidade_por_uf/gold"


def main():
    """Agrega os óbitos e a taxa média de mortalidade por UF."""
    spark = SparkSession.builder.appName("datasus-mortalidade-por-uf-gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consolidado = spark.read.parquet(ORIGEM)

    gold = (
        consolidado.groupBy("uf")
        .agg(
            F.sum("obitos").cast("long").alias("obitos_total"),
            F.round(F.avg("taxa_100k"), 2).alias("taxa_media"),
        )
        .orderBy(F.col("obitos_total").desc())
    )

    gold.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> gold datasus_mortalidade_por_uf: {n} UFs em {DESTINO}")
    gold.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
