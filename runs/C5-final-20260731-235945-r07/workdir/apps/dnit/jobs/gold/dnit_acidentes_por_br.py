from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils.metadata import table_location

ORIGEM = table_location("dnit", "acidente_geo", "silver")
DESTINO = table_location("dnit", "acidentes_por_br", "gold")


def main():
    """Aggregate accident counts and victims per federal highway and state."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("dnit-acidentes-por-br-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    acidentes = spark.read.format("geoparquet").load(ORIGEM)

    gold = (
        acidentes.groupBy("br", "uf")
        .agg(
            F.count(F.lit(1)).cast("long").alias("total_acidentes"),
            F.sum("mortos").cast("long").alias("total_mortos"),
            F.sum("feridos").cast("long").alias("total_feridos"),
        )
        .orderBy(F.col("total_mortos").desc())
    )

    gold.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> gold dnit_acidentes_por_br: {n} BRs em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
