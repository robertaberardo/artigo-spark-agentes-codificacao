from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("anac", "aerodromo", "bronze")
DESTINO = table_location("anac", "aerodromo_geo", "silver")

BRASILIA_LAT, BRASILIA_LON = -15.7997, -47.8645


def main():
    """Aeródromos georreferenciados: ponto, distância a Brasília, geometria 3857."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("anac-aerodromo-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    aerodromo = spark.read.parquet(ORIGEM)

    geo = (
        aerodromo.transform(
            lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude")
        )
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        # Distância geodésica (m) sobre o ponto WGS84 ORIGINAL, ANTES de projetar
        # para 3857. Nunca reprojetar o ponto ida-e-volta só para medir — o
        # round-trip 4326→3857→4326 perde precisão. ST_DistanceSpheroid opera no
        # elipsoide WGS84 direto; só a geometria PERSISTIDA vai para 3857 depois.
        .withColumn(
            "distance_brasilia_m",
            stf.ST_DistanceSpheroid(
                F.col("geometry"), stc.ST_Point(F.lit(BRASILIA_LON), F.lit(BRASILIA_LAT))
            ),
        )
        .transform(dt.to_web_mercator)
        .select(
            "codigo_oaci",
            "nome",
            "uf",
            "tipo",
            "altitude_m",
            "distance_brasilia_m",
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver anac_aerodromo_geo: {n} aeródromos em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
