from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_geo", "silver")

# Códigos do Censo Escolar filtrados nesta silver.
SETOR_FEDERAL = 1
SITUACAO_EM_ATIVIDADE = 1


def keep_federal_active(df: DataFrame) -> DataFrame:
    """Mantém apenas escolas federais em atividade."""
    is_federal = F.col("setor") == SETOR_FEDERAL
    is_active = F.col("situacao") == SITUACAO_EM_ATIVIDADE
    return df.filter(is_federal & is_active)


def attach_enrollment(escola: DataFrame, matricula: DataFrame) -> DataFrame:
    """Anexa o total de matrículas da educação básica de cada escola.

    ``left join`` pela chave da unidade: escolas sem linha de matrícula ficam
    com ``total_enrollment`` nulo (ausência não é zero).
    """
    enrollment = matricula.select(
        F.col("id_unidade"),
        F.col("mat_basica").alias("total_enrollment"),
    )
    return escola.join(enrollment, on="id_unidade", how="left")


def main():
    """Escolas federais em atividade georreferenciadas (ponto, matrículas, 3857)."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA).transform(keep_federal_active)
    matricula = spark.read.parquet(MATRICULA)

    geo = (
        attach_enrollment(escola, matricula)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        # Preserva lat/long WGS84 originais: as distâncias geodésicas do gold são
        # medidas sobre o ponto original, antes de qualquer reprojeção (evita o
        # round-trip 4326→3857→4326). A geometria persistida vai para 3857.
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "uf",
            "cod_municipio",
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
