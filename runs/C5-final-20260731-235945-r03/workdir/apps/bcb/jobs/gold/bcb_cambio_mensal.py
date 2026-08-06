from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("bcb", "serie_diaria", "silver")
DESTINO = table_location("bcb", "cambio_mensal", "gold")


def main():
    """Aggregate the daily FX series into monthly averages per currency."""
    spark = SparkSession.builder.appName("bcb-cambio-mensal-gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    serie = spark.read.parquet(ORIGEM)

    monthly = (
        serie.withColumn("month", F.date_format(F.col("data"), "yyyy-MM"))
        .withColumn("year", ct.extract_year(F.col("data")))
        .withColumn("mid", (F.col("compra") + F.col("venda")) / F.lit(2.0))
        .groupBy("month", "year", "moeda")
        .agg(
            F.round(F.avg("compra"), 4).alias("avg_buy"),
            F.round(F.avg("venda"), 4).alias("avg_sell"),
            F.round(F.avg("mid"), 4).alias("media_mensal"),
        )
        .select(
            "month",
            "year",
            F.col("moeda").alias("currency"),
            "avg_buy",
            "avg_sell",
            "media_mensal",
        )
        .orderBy("month", "currency")
    )

    monthly.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> gold bcb_cambio_mensal: {n} rows em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
