from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_geo", "silver")

# Códigos do Censo Escolar: setor 1 = federal; situação 1 = em atividade.
FEDERAL_SETOR = 1
ACTIVE_SITUACAO = 1


def main():
    """Escolas federais em atividade, georreferenciadas e com o total de matrículas.

    Preserva latitude/longitude WGS84 originais para que o gold meça distância
    geodésica sobre elas; a geometria de saída fica em EPSG:3857 (convenção silver).
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    enrollment = spark.read.parquet(MATRICULA).select(
        "id_unidade", F.col("mat_basica").alias("total_enrollment")
    )

    is_federal = F.col("setor") == FEDERAL_SETOR
    is_active = F.col("situacao") == ACTIVE_SITUACAO
    federal_active = escola.filter(is_federal & is_active)

    geo = (
        federal_active.transform(
            lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude")
        )
        .transform(lambda d: dt.join_no_fanout(d, enrollment, "id_unidade", "left"))
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
