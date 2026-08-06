from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("inmet", "estacao", "bronze")
DESTINO = table_location("inmet", "estacao_geo", "silver")


def main():
    """Estações georreferenciadas: ponto lat/lon a partir do cadastro bronze."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("inmet-estacao-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    estacao = spark.read.parquet(ORIGEM)

    geo = (
        estacao.transform(
            lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude")
        )
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .select("codigo_wmo", "nome", "uf", "altitude", "geometry")
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver inmet_estacao_geo: {n} estações em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
