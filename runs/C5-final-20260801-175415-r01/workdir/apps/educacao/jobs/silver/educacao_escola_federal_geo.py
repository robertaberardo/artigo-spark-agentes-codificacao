from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from apps.educacao.utils import column_transforms as educ_ct
from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_federal_geo", "silver")


def main():
    """Escolas federais em atividade: matrículas juntadas, ponto e geometria 3857.

    Guarda também latitude/longitude em WGS84 para que a camada gold possa medir
    distâncias geodésicas sobre as coordenadas originais.
    """
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-federal-geo-silver")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA)

    is_federal = educ_ct.is_federal(F.col("setor"))
    is_active = educ_ct.is_active(F.col("situacao"))
    enrollment = matricula.select(
        F.col("id_unidade"),
        F.col("mat_basica").alias("total_enrollment"),
    )

    geo = (
        escola.filter(is_federal & is_active)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .transform(lambda d: dt.join_no_fanout(d, enrollment, "id_unidade", "left"))
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "nome_unidade",
            "uf",
            "latitude",
            "longitude",
            F.col("total_enrollment").cast("int").alias("total_enrollment"),
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_federal_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
