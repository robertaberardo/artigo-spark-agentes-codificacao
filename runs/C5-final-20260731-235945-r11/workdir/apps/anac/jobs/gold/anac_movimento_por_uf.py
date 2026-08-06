from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from utils.metadata import table_location

ORIGEM = table_location("anac", "voo_consolidado", "silver")
DESTINO = table_location("anac", "movimento_por_uf", "gold")


def main():
    """Agrega o movimento de voos por UF de origem."""
    spark = SparkSession.builder.appName("anac-movimento-por-uf-gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    voos = spark.read.parquet(ORIGEM)

    gold = (
        voos.groupBy(F.col("origem_uf").alias("uf"))
        .agg(
            F.count(F.lit(1)).alias("voos"),
            F.sum("passageiros").cast("long").alias("passageiros_total"),
            F.round(F.avg("load_factor"), 2).alias("load_factor_medio"),
        )
        .orderBy(F.col("passageiros_total").desc())
    )

    gold.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> gold anac_movimento_por_uf: {n} UFs em {DESTINO}")
    gold.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
