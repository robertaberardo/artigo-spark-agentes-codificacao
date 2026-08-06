from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils import dataframe_transforms as dt
from utils.metadata import table_location

VOTACAO = table_location("tse", "votacao", "bronze")
CANDIDATO = table_location("tse", "candidato", "bronze")
PARTIDO = table_location("tse", "partido", "bronze")
DESTINO = table_location("tse", "votacao_consolidada", "silver")


def main():
    """Votos totalizados por candidato, com dados de candidato e partido."""
    spark = SparkSession.builder.appName("tse-votacao-consolidada-silver").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    votacao = spark.read.parquet(VOTACAO)
    candidato = spark.read.parquet(CANDIDATO)
    partido = spark.read.parquet(PARTIDO)

    votos_por_candidato = votacao.groupBy("id_candidato").agg(
        F.sum(F.col("votos")).cast("long").alias("votos")
    )

    candidato_cols = candidato.select(
        F.col("id_candidato"),
        F.col("nome"),
        F.col("partido_sigla"),
        F.col("uf"),
        F.col("cargo"),
    )
    consolidado = dt.join_no_fanout(
        votos_por_candidato, candidato_cols, on="id_candidato", how="inner"
    )

    partido_cols = partido.select(
        F.col("sigla").alias("partido_sigla"),
        F.col("nome").alias("partido_nome"),
    )
    consolidado = dt.join_no_fanout(consolidado, partido_cols, on="partido_sigla", how="left")

    result = consolidado.select(
        F.col("id_candidato"),
        F.col("nome"),
        F.col("partido_sigla"),
        F.col("partido_nome"),
        F.col("uf"),
        F.col("cargo"),
        F.col("votos"),
    )

    result.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> silver tse_votacao_consolidada: {n} candidatos em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
