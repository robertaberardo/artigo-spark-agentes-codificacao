from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_federal_geo", "silver")

# Códigos do censo escolar (ver metadata.toml).
SETOR_FEDERAL = "1"
SITUACAO_ATIVA = "1"


def select_federal_active(df):
    """Mantém apenas as escolas federais (setor 1) em atividade (situação 1)."""
    is_federal = F.col("setor") == SETOR_FEDERAL
    is_active = F.col("situacao") == SITUACAO_ATIVA
    return df.filter(is_federal & is_active)


def main():
    """Escolas federais em atividade, georreferenciadas e com matrículas.

    Preserva latitude/longitude (WGS84) para o cálculo geodésico no gold; a
    geometria de saída vai em EPSG:3857 por convenção do lake.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-federal-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA).select(
        F.col("id_unidade"), F.col("mat_basica").alias("enrollment")
    )

    geo = (
        escola.transform(select_federal_active)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .transform(
            lambda d: dt.join_no_fanout(d, matricula, "id_unidade", "left")
        )
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "nome_unidade",
            "uf",
            "enrollment",
            "latitude",
            "longitude",
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_federal_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
