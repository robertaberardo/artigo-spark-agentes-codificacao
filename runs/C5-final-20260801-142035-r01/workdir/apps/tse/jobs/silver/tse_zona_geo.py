from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("tse", "zona", "bronze")
DESTINO = table_location("tse", "zona_geo", "silver")


def main():
    """Zonas eleitorais como ponto (lat/lon) reprojetado para EPSG:3857."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("tse-zona-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    zona = spark.read.parquet(ORIGEM)

    geo = (
        zona.transform(
            lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude")
        )
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_zona",
            "municipio_ibge",
            "uf",
            "eleitores",
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver tse_zona_geo: {n} zonas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
