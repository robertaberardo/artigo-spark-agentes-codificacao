from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_geo", "silver")


def main():
    """Consolida escola + matrículas, georreferencia o ponto e grava a silver.

    Mantém ``latitude``/``longitude`` originais (WGS84) além da geometria em
    EPSG:3857: a medição de distância no gold é geodésica sobre o ponto original,
    nunca sobre o round-trip da projeção métrica.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA).select(
        "id_unidade", F.col("mat_basica").alias("enrollment")
    )

    enriched = dt.join_no_fanout(escola, matricula, on="id_unidade", how="left")

    geo = (
        enriched
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "uf",
            "nome_municipio",
            "setor",
            "situacao",
            "enrollment",
            "latitude",
            "longitude",
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
