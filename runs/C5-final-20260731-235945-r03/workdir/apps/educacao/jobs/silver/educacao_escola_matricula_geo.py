from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from apps.educacao.utils import column_transforms as edu_ct
from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula_geo", "silver")


def keep_federal_active(escola):
    """Mantém apenas as escolas federais em atividade."""
    federal = edu_ct.is_federal(F.col("setor"))
    active = edu_ct.is_active(F.col("situacao"))
    return escola.filter(federal & active)


def main():
    """Silver: escolas federais em atividade, georreferenciadas e com matrículas.

    O total de matrículas vem de ``mat_basica`` (educação básica). O join é
    ``left`` para não descartar escola sem registro de matrícula — nesse caso
    ``enrollment`` fica ``NULL`` (ausência), nunca zero.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA).select(
        "id_unidade", F.col("mat_basica").alias("enrollment")
    )

    escola = (
        escola.transform(keep_federal_active)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
    )

    joined = dt.join_no_fanout(escola, matricula, on="id_unidade", how="left")

    geo = (
        joined.transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "uf",
            "enrollment",
            "latitude",
            "longitude",
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_matricula_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
