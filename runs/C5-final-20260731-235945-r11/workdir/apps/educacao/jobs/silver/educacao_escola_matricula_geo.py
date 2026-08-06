from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula_geo", "silver")


def attach_enrollment(escola, matricula):
    """Anexa a matrícula da educação básica a cada escola (left join sem fan-out).

    A chave ``id_unidade`` é única em matrícula; ``join_no_fanout`` valida isso e
    falha cedo caso surja duplicidade, em vez de multiplicar linhas.
    """
    enrollment = matricula.select(
        "id_unidade", F.col("mat_basica").alias("enrollment")
    )
    return dt.join_no_fanout(escola, enrollment, on="id_unidade", how="left")


def main():
    """Escolas georreferenciadas com matrícula: ponto WGS84 preservado, geometria 3857."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA)

    geo = (
        attach_enrollment(escola, matricula)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        # latitude/longitude seguem para o gold porque as distâncias geodésicas são
        # medidas sobre o ponto WGS84 ORIGINAL — a geometria 3857 é só convenção de saída.
        .select(
            "id_unidade",
            "uf",
            "nome_municipio",
            "setor",
            "situacao",
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
