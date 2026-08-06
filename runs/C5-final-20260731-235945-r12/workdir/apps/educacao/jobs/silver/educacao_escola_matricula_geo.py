from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula_geo", "silver")

# Setor 1 = federal; situação 1 = em atividade.
SETOR_FEDERAL = 1
SITUACAO_ATIVA = 1


def main():
    """Escolas federais em atividade, com matrículas e ponto georreferenciado.

    Filtra as federais em atividade, junta as matrículas (educação básica) pela
    unidade e materializa o ponto em EPSG:3857. Mantém latitude/longitude em
    WGS84 para que o gold meça distâncias geodésicas sobre as coordenadas
    originais, sem round-trip por projeção.
    """
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-geo-silver")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA)

    federais = (
        escola.filter(
            (F.col("setor") == SETOR_FEDERAL)
            & (F.col("situacao") == SITUACAO_ATIVA)
        )
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .select("id_unidade", "uf", "latitude", "longitude")
    )

    enrollment = matricula.select(
        "id_unidade", F.col("mat_basica").alias("enrollment")
    )

    geo = (
        dt.join_lookup(
            federais, enrollment, on="id_unidade", columns=["enrollment"], how="left"
        )
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select("id_unidade", "uf", "enrollment", "latitude", "longitude", "geometry")
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_matricula_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
