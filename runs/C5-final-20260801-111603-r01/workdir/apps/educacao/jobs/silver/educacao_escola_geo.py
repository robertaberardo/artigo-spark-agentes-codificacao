from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_geo", "silver")

SETOR_FEDERAL = 1
SITUACAO_EM_ATIVIDADE = 1


def filter_federal_ativa(df):
    """Mantém apenas escolas federais (setor 1) em atividade (situação 1)."""
    is_federal = F.col("setor") == SETOR_FEDERAL
    is_ativa = F.col("situacao") == SITUACAO_EM_ATIVIDADE
    return df.filter(is_federal & is_ativa)


def main():
    """Escolas federais em atividade, georreferenciadas e com matrículas.

    Preserva latitude/longitude em WGS84 além da geometria persistida em 3857:
    a grade H3 e as distâncias geodésicas do gold são calculadas sobre o CRS
    geográfico original, sem o round-trip 4326→3857→4326 (que perderia precisão)
    nem indexar H3 sobre coordenadas métricas (que produziria células erradas).
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA).select("id_unidade", "mat_basica")

    geo = (
        escola.transform(filter_federal_ativa)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(lambda d: dt.join_lookup(d, matricula, "id_unidade", ["mat_basica"]))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "uf",
            F.col("mat_basica").alias("enrollment"),
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
