from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_geo", "silver")


def main():
    """Escolas georreferenciadas com matrículas: ponto WGS84 projetado para 3857.

    Mantém latitude/longitude originais (WGS84) além da geometria em 3857 — o gold
    mede distâncias geodésicas sobre as coordenadas originais, sem round-trip de
    reprojeção. A matrícula da educação básica (``mat_basica``) entra como total de
    matrículas; escola sem registro de matrícula fica com ``NULL`` (ausência, não 0).
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA).select(
        F.col("id_unidade"),
        F.col("mat_basica").alias("total_enrollment"),
    )

    geo = (
        escola.transform(
            lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude")
        )
        .transform(lambda d: dt.join_no_fanout(d, matricula, "id_unidade", "left"))
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "uf",
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
    print(f">> silver educacao_escola_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
