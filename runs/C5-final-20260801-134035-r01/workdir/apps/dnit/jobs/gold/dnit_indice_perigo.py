from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils.metadata import table_location

ORIGEM = table_location("dnit", "acidente_geo", "silver")
DESTINO = table_location("dnit", "indice_perigo", "gold")

PESO_MORTO = 3


def main():
    """Índice de perigo por BR/UF, ponderando mortos e feridos, com faixa."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("dnit-indice-perigo-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    acidentes = spark.read.format("geoparquet").load(ORIGEM)

    agregado = acidentes.groupBy("br", "uf").agg(
        F.sum("mortos").cast("long").alias("mortos_total"),
        F.sum("feridos").cast("long").alias("feridos_total"),
    )

    indice = (
        F.col("mortos_total") * F.lit(PESO_MORTO) + F.col("feridos_total")
    )
    com_indice = agregado.withColumn("indice_perigo", indice.cast("long"))

    faixa = (
        F.when(F.col("indice_perigo") < F.lit(50), F.lit("baixo"))
        .when(F.col("indice_perigo") < F.lit(200), F.lit("medio"))
        .otherwise(F.lit("alto"))
    )
    gold = com_indice.withColumn("faixa", faixa).orderBy(
        F.col("indice_perigo").desc()
    )

    gold.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> gold dnit_indice_perigo: {n} BRs em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
