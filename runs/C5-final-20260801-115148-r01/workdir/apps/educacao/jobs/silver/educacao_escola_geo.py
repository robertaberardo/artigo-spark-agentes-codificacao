from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_geo", "silver")

SETOR_FEDERAL = "1"
SITUACAO_ATIVA = "1"


def filter_federal_active(df):
    """Mantém apenas escolas do setor federal em atividade (SETOR=1, SITUACAO=1)."""
    is_federal = F.col("setor") == SETOR_FEDERAL
    is_active = F.col("situacao") == SITUACAO_ATIVA
    return df.filter(is_federal & is_active)


def main():
    """Escolas federais em atividade, georreferenciadas e com o total de matrículas.

    Filtra federais ativas com coordenada válida, junta o total de matrículas da
    educação básica (``mat_basica``) por unidade e persiste o ponto em EPSG:3857.
    ``latitude``/``longitude`` originais (WGS84) seguem no schema porque as
    distâncias do gold são medidas sobre as coordenadas originais, não sobre a
    projeção métrica.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA)

    federal = (
        escola.transform(filter_federal_active)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
    )
    enrollment = matricula.select(
        F.col("id_unidade"), F.col("mat_basica").alias("total_enrollment")
    )

    geo = (
        dt.join_no_fanout(federal, enrollment, "id_unidade", "left")
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "uf",
            "total_enrollment",
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
