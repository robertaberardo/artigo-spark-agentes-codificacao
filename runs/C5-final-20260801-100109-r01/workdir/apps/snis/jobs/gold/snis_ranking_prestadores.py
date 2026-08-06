from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from apps.snis.utils import column_transforms as snis_ct
from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("snis", "prestador_consolidado", "silver")
DESTINO = table_location("snis", "ranking_prestadores", "gold")


def main():
    """Rank sanitation providers within each state by their coverage scope."""
    spark = SparkSession.builder.appName("snis-ranking-prestadores-gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    prestadores = spark.read.parquet(ORIGEM)

    scored = prestadores.withColumn(
        "peso_abrangencia", snis_ct.abrangencia_weight(F.col("abrangencia"))
    )

    result = (
        dt.rank_within_group(
            scored, partition_cols=["uf"], order_col="peso_abrangencia", out_col="rank"
        )
        .select(
            "codigo",
            "nome",
            "uf",
            "natureza_juridica",
            "abrangencia",
            "peso_abrangencia",
            "rank",
        )
        .orderBy(F.col("uf").asc(), F.col("rank").asc())
    )

    result.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> gold snis_ranking_prestadores: {n} providers em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
