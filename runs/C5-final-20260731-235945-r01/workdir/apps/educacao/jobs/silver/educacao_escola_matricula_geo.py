from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula_geo", "silver")

# códigos do censo: apenas rede federal (1) em atividade (1)
FEDERAL_SETOR = "1"
ACTIVE_SITUACAO = "1"


def keep_federal_active(df: DataFrame) -> DataFrame:
    """Mantém apenas escolas da rede federal em atividade."""
    is_federal = F.col("setor") == FEDERAL_SETOR
    is_active = F.col("situacao") == ACTIVE_SITUACAO
    return df.filter(is_federal & is_active)


def main():
    """Escolas federais em atividade, com matrícula e ponto georreferenciado."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA).transform(keep_federal_active)
    # a matrícula da educação básica é o total de matrículas da escola
    matricula = spark.read.parquet(MATRICULA).select(
        "id_unidade", F.col("mat_basica").alias("total_enrollment")
    )

    geo = (
        dt.join_no_fanout(escola, matricula, on="id_unidade", how="left")
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
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
    print(f">> silver educacao_escola_matricula_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
