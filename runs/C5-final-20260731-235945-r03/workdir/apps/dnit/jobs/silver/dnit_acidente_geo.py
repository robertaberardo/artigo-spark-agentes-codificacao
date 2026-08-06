from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt

ORIGEM = "s3a://datalake/dnit/dnit_acidente/bronze"
DESTINO = "s3a://datalake/dnit/dnit_acidente_geo/silver"


def main():
    """Acidentes georreferenciados: ponto a partir de lat/long, geometria 3857."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("dnit-acidente-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    acidente = spark.read.parquet(ORIGEM)

    geo = (
        acidente.transform(
            lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude")
        )
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id",
            "br",
            "uf",
            "km",
            "data",
            "mortos",
            "feridos",
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver dnit_acidente_geo: {n} acidentes em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
