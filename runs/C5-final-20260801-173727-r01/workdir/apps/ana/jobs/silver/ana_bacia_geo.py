from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt

BACIA = "s3a://datalake/ana/ana_bacia/bronze"
DESTINO = "s3a://datalake/ana/ana_bacia_geo/silver"


def main():
    """Bacias georreferenciadas: geometria do WKT, reprojeção 3857 e área em km²."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("ana-bacia-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    bacia = spark.read.parquet(BACIA)

    geo = (
        bacia.transform(lambda d: dt.add_line_geometry(d, "geom_wkt"))
        .transform(dt.to_web_mercator)
        .transform(dt.add_area_km2)
        .select("codigo_bacia", "nome", "area_km2", "geometry")
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver ana_bacia_geo: {n} bacias em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
