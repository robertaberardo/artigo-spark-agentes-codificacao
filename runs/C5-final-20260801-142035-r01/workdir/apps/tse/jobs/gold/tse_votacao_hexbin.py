from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("tse", "zona_geo", "silver")
DESTINO = table_location("tse", "votacao_hexbin", "gold")

H3_LEVEL = 6


def main():
    """Distribuição do eleitorado por célula H3 nível 6."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("tse-votacao-hexbin-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    zona_geo = spark.read.format("geoparquet").load(ORIGEM)

    indexed = dt.attach_h3_index(zona_geo, "geometry", H3_LEVEL, "h3_cell")

    hexbin = dt.aggregate_by_h3(
        indexed,
        "h3_cell",
        [
            F.sum(indexed["eleitores"]).cast("long").alias("eleitores"),
            F.count(F.lit(1)).alias("zonas"),
        ],
    )

    result = hexbin.select(
        hexbin["h3_cell"],
        hexbin["eleitores"],
        hexbin["zonas"],
        hexbin["geometry"],
    )

    result.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold tse_votacao_hexbin: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
