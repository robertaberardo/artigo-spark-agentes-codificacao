from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils import column_transforms as ct
from utils import dataframe_transforms as dt
from utils.metadata import table_location

ZONA = table_location("tse", "zona", "bronze")
VOTACAO = table_location("tse", "votacao", "bronze")
DESTINO = table_location("tse", "comparecimento_por_uf", "gold")


def main():
    """Comparecimento (votos sobre eleitores) agregado por UF."""
    spark = SparkSession.builder.appName("tse-comparecimento-por-uf-gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    zona = spark.read.parquet(ZONA)
    votacao = spark.read.parquet(VOTACAO)

    eleitores = zona.groupBy("uf").agg(
        F.sum(F.col("eleitores")).cast("long").alias("eleitores")
    )
    votos = votacao.groupBy("uf").agg(
        F.sum(F.col("votos")).cast("long").alias("votos")
    )

    gold = (
        dt.join_lookup(eleitores, votos, "uf", how="left")
        .withColumn(
            "comparecimento",
            F.round(ct.safe_divide(F.col("votos"), F.col("eleitores")), 4),
        )
        .select("uf", "votos", "eleitores", "comparecimento")
        .orderBy(F.col("comparecimento").desc())
    )

    gold.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> gold tse_comparecimento_por_uf: {n} UFs em {DESTINO}")
    gold.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
