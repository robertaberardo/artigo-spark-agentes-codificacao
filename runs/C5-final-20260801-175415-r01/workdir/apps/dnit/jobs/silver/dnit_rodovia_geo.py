from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("dnit", "rodovia", "bronze")
DESTINO = table_location("dnit", "rodovia_geo", "silver")


def main():
    """Rodovias georreferenciadas: traçado em WKT e comprimento geodésico."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("dnit-rodovia-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    rodovia = spark.read.parquet(ORIGEM)

    geo = (
        rodovia.transform(lambda d: dt.add_line_geometry(d, "geom_wkt"))
        .withColumn("comprimento_m", stf.ST_LengthSpheroid(F.col("geometry")))
        .select(
            "sigla_br",
            "uf",
            "extensao_km",
            "comprimento_m",
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver dnit_rodovia_geo: {n} rodovias em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
