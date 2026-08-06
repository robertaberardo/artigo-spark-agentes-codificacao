from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("ana", "estacao_geo", "silver")
DESTINO = table_location("ana", "estacoes_hexbin", "gold")

H3_LEVEL = 6


def main():
    """Densidade de estações fluviométricas por célula H3 nível 6."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("ana-estacoes-hexbin-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    geo = spark.read.format("geoparquet").load(ORIGEM)

    indexed = geo.transform(
        lambda d: dt.attach_h3_index(d, "geometry", H3_LEVEL, "h3_cell")
    )

    hexbin = dt.aggregate_by_h3(
        indexed,
        "h3_cell",
        [
            F.count(F.lit(1)).alias("estacoes"),
            F.round(F.avg("distance_brasilia_m"), 1).alias("distance_media_m"),
        ],
    )

    hexbin.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold ana_estacoes_hexbin: {n} células em {DESTINO}")
    spark.stop()


main()
