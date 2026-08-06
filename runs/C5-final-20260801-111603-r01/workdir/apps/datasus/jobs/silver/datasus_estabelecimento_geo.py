from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("datasus", "estabelecimento", "bronze")
DESTINO = table_location("datasus", "estabelecimento_geo", "silver")

BRASILIA_LAT, BRASILIA_LON = -15.7997, -47.8645


def main():
    """Estabelecimentos georreferenciados: ponto, distância a Brasília, geometria 3857."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("datasus-estabelecimento-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    estabelecimento = spark.read.parquet(ORIGEM)

    geo = (
        estabelecimento.transform(
            lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude")
        )
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(
            lambda d: dt.add_distance_to_reference(
                d, BRASILIA_LAT, BRASILIA_LON, out_col="distance_brasilia_m"
            )
        )
        .transform(dt.to_web_mercator)
        .select(
            "co_cnes",
            "nome",
            "uf",
            "tipo",
            "leitos_sus",
            "distance_brasilia_m",
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver datasus_estabelecimento_geo: {n} estabelecimentos em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
