from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula", "silver")


def main():
    """Escolas georreferenciadas com matrículas: ponto WGS84 + geometria 3857.

    Mantém ``latitude``/``longitude`` originais para que a gold meça distâncias
    geodésicas sobre o ponto WGS84 (``ST_DistanceSpheroid``) sem round-trip de
    reprojeção; a geometria persistida vai para EPSG:3857, como no restante do
    lake.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA)

    # matrícula é única por id_unidade; join_no_fanout falha cedo se não for.
    total_enrollment = matricula.select(
        "id_unidade", F.col("mat_basica").alias("total_enrollment")
    )

    geo = (
        dt.join_no_fanout(escola, total_enrollment, on="id_unidade", how="left")
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "nome_unidade",
            "uf",
            "nome_municipio",
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
    print(f">> silver educacao_escola_matricula: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
