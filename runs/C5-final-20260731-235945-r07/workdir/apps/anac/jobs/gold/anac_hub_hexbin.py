from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("anac", "aerodromo_geo", "silver")
DESTINO = table_location("anac", "hub_hexbin", "gold")

H3_LEVEL = 6


def main():
    """Densidade de aeródromos por célula H3 nível 6."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("anac-hub-hexbin-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    geo = spark.read.format("geoparquet").load(ORIGEM)

    indexed = geo.select("*").withColumn(
        "h3_cell", F.element_at(stf.ST_H3CellIDs(F.col("geometry"), H3_LEVEL, False), 1)
    )

    hexbin = (
        indexed.groupBy(indexed["h3_cell"])
        .agg(
            F.count(F.lit(1)).alias("aerodromos"),
            F.round(F.avg(indexed["altitude_m"]), 1).alias("altitude_media_m"),
        )
        .withColumn(
            "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
        )
    )

    hexbin.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold anac_hub_hexbin: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
