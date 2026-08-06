from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_geo", "silver")

# Códigos do Censo: setor 1 = federal; situação 1 = em atividade.
SETOR_FEDERAL = "1"
SITUACAO_ATIVA = "1"


def keep_federal_active(df: DataFrame) -> DataFrame:
    """Mantém apenas as escolas do setor federal em atividade."""
    is_federal = F.col("setor") == SETOR_FEDERAL
    is_active = F.col("situacao") == SITUACAO_ATIVA
    return df.filter(is_federal & is_active)


def attach_enrollment(escola: DataFrame, matricula: DataFrame) -> DataFrame:
    """Enriquece as escolas com o total de matrículas (mat_basica) da unidade.

    ``left join`` para preservar toda escola federal em atividade mesmo sem
    registro de matrícula; a ausência fica ``NULL`` (não zero). ``join_no_fanout``
    garante que a matrícula é única por unidade e não multiplica linhas.
    """
    enrollment = matricula.select(
        "id_unidade", F.col("mat_basica").alias("total_enrollment")
    )
    return dt.join_no_fanout(escola, enrollment, on="id_unidade", how="left")


def main():
    """Escolas federais em atividade, georreferenciadas e com total de matrículas.

    Guarda lat/long WGS84 originais (para medir distâncias geodésicas no gold,
    antes de qualquer projeção) e a geometria do ponto em EPSG:3857, convenção
    de saída do lake.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA)

    geo = (
        escola.transform(keep_federal_active)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .transform(lambda d: attach_enrollment(d, matricula))
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "uf",
            "latitude",
            "longitude",
            "total_enrollment",
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
