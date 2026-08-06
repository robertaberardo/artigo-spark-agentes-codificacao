from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils import column_transforms as ct
from utils import dataframe_transforms as dt
from utils.metadata import table_location

LEITO = table_location("datasus", "leito", "bronze")
ESTABELECIMENTO = table_location("datasus", "estabelecimento", "bronze")
DESTINO = table_location("datasus", "densidade_leitos", "gold")


def main():
    """Densidade de leitos por UF: total de leitos por estabelecimento."""
    spark = SparkSession.builder.appName("datasus-densidade-leitos-gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    leito = spark.read.parquet(LEITO)
    estabelecimento = spark.read.parquet(ESTABELECIMENTO).select(
        F.col("co_cnes"), F.col("uf")
    )

    gold = (
        leito.transform(lambda d: dt.join_lookup(d, estabelecimento, "co_cnes"))
        .groupBy("uf")
        .agg(
            F.countDistinct("co_cnes").cast("long").alias("estabelecimentos"),
            F.sum("quantidade").cast("long").alias("leitos_total"),
        )
        .withColumn(
            "densidade_leitos",
            F.round(
                ct.safe_divide(F.col("leitos_total"), F.col("estabelecimentos")), 2
            ),
        )
        .orderBy(F.col("leitos_total").desc())
    )

    gold.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> gold datasus_densidade_leitos: {n} UFs em {DESTINO}")
    gold.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
