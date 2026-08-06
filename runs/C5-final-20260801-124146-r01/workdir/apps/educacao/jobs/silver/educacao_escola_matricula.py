from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula", "silver")

COLUNAS = [
    "id_unidade",
    "uf",
    "setor",
    "situacao",
    "latitude",
    "longitude",
    "matriculas",
    "geometry",
]


def main():
    """Consolida escolas + matrículas georreferenciadas (ponto em EPSG:3857).

    A lat/long WGS84 original é preservada nas colunas ``latitude``/``longitude``
    para que a gold meça distâncias geodésicas antes de qualquer projeção.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    # matrícula da educação básica é o total de matrículas da escola
    matricula = spark.read.parquet(MATRICULA).select(
        "id_unidade", F.col("mat_basica").alias("matriculas")
    )

    consolidado = (
        escola.transform(
            lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude")
        )
        .transform(
            lambda d: dt.join_lookup(d, matricula, "id_unidade", ["matriculas"])
        )
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(*COLUNAS)
    )

    consolidado.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_matricula: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
