from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_geo", "silver")


def attach_enrollment(escola: DataFrame, matricula: DataFrame) -> DataFrame:
    """Anexa o total de matrículas (educação básica) por escola.

    ``left join`` para preservar toda escola georreferenciada, mesmo sem
    registro de matrícula — nesse caso ``total_enrollment`` fica ``NULL``
    (ausência não é zero). A chave ``id_unidade`` é única na matrícula, então o
    join não deve multiplicar linhas (``join_no_fanout`` valida).
    """
    enrollment = matricula.select(
        F.col("id_unidade"),
        F.col("mat_basica").alias("total_enrollment"),
    )
    return dt.join_no_fanout(escola, enrollment, on="id_unidade", how="left")


def main():
    """Escolas georreferenciadas com matrículas: ponto WGS84 e geometria 3857."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA)

    geo = (
        escola.transform(
            lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude")
        )
        .transform(lambda d: attach_enrollment(d, matricula))
        # latitude/longitude WGS84 originais permanecem no schema para a medida
        # geodésica no gold; só a geometria persistida vai para 3857.
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "nome_unidade",
            "uf",
            "cod_municipio",
            "setor",
            "situacao",
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
