from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt

ORIGEM = "s3a://datalake/tse/tse_municipio/bronze"
DESTINO = "s3a://datalake/tse/tse_municipio_geo/silver"


def main():
    """Municípios como polígono (EPSG:3857) com área em km²."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("tse-municipio-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    municipio = spark.read.parquet(ORIGEM)

    geo = (
        municipio.transform(lambda d: dt.add_line_geometry(d, "geom_wkt"))
        .transform(dt.to_web_mercator)
        .transform(dt.add_area_km2)
        .select(
            "municipio_ibge",
            "nome",
            "uf",
            "area_km2",
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver tse_municipio_geo: {n} municípios em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
