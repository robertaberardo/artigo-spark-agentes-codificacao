from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula_geo", "silver")


def main():
    """Escolas georreferenciadas com o total de matrículas da educação básica.

    Filtra coordenadas válidas, enriquece com ``MAT_BASICA`` (join sem fan-out) e
    persiste o ponto em EPSG:3857. As colunas ``latitude``/``longitude`` originais
    (WGS84) são preservadas para medições geodésicas no gold, sem round-trip de CRS.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA).select("id_unidade", "mat_basica")

    geo = (
        escola.transform(
            lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude")
        )
        .transform(
            lambda d: dt.join_no_fanout(d, matricula, on="id_unidade", how="left")
        )
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "uf",
            "setor",
            "situacao",
            "latitude",
            "longitude",
            F.col("mat_basica").alias("total_enrollment"),
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_matricula_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
