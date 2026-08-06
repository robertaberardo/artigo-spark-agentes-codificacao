from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_geo", "silver")

# A geometria fica em WGS84 (EPSG:4326): o gold indexa H3 e mede distância
# geodésica sobre as coordenadas originais, sem round-trip por 3857.


def main():
    """Escolas georreferenciadas (ponto WGS84) enriquecidas com as matrículas."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA).select(
        F.col("id_unidade"),
        F.col("mat_basica").alias("enrollment"),
    )

    escola_valida = escola.transform(
        lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude")
    )

    enriquecida = dt.join_no_fanout(
        escola_valida, matricula, on="id_unidade", how="left"
    )

    geo = (
        enriquecida.transform(
            lambda d: dt.add_point_geometry(d, "latitude", "longitude")
        ).select(
            F.col("id_unidade"),
            F.col("uf"),
            F.col("setor"),
            F.col("situacao"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("enrollment"),
            F.col("geometry"),
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
