from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula_geo", "silver")

SETOR_FEDERAL = 1
SITUACAO_ATIVA = 1


def keep_federal_active(df: DataFrame) -> DataFrame:
    """Mantém apenas as escolas federais (``setor``) em atividade (``situacao``)."""
    is_federal = F.col("setor") == SETOR_FEDERAL
    is_active = F.col("situacao") == SITUACAO_ATIVA
    return df.filter(is_federal & is_active)


def main():
    """Escolas federais em atividade, georreferenciadas e com o total de matrículas.

    Preserva ``latitude``/``longitude`` originais: o gold reconstrói o ponto WGS84
    a partir delas para medir distâncias geodésicas, evitando o round-trip
    3857→4326 (a geometria é gravada em 3857 apenas como convenção de saída).
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = (
        spark.read.parquet(ESCOLA)
        .transform(keep_federal_active)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
    )
    matricula = spark.read.parquet(MATRICULA).select(
        F.col("id_unidade"),
        F.col("mat_basica").alias("total_enrollment"),
    )

    # left join: uma escola federal ativa sem registro de matrícula continua na
    # base (total_enrollment fica nulo — ausência não é zero).
    geo = (
        escola.join(matricula, on="id_unidade", how="left")
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "nome_unidade",
            "uf",
            "nome_municipio",
            "latitude",
            "longitude",
            "total_enrollment",
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_matricula_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
