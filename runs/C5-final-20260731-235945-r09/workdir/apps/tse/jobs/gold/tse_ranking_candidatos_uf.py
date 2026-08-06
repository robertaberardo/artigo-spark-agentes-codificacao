from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("tse", "votacao_consolidada", "silver")
DESTINO = table_location("tse", "ranking_candidatos_uf", "gold")


def main():
    """Ranqueia os candidatos por votos totalizados dentro de cada UF."""
    spark = SparkSession.builder.appName("tse-ranking-candidatos-uf-gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consolidado = spark.read.parquet(ORIGEM)

    ranking = dt.rank_within_group(
        consolidado,
        partition_cols=["uf"],
        order_col="votos",
        out_col="rank_uf",
    ).select(
        "id_candidato",
        "nome",
        "partido_sigla",
        "uf",
        "votos",
        "rank_uf",
    )

    result = ranking.orderBy(F.col("uf").asc(), F.col("rank_uf").asc())

    result.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> gold tse_ranking_candidatos_uf: {n} candidatos em {DESTINO}")
    result.show(10, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
