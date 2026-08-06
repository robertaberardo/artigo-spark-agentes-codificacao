from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("inmet", "estacao_geo", "silver")
DESTINO = table_location("inmet", "estacoes_hexbin", "gold")

H3_LEVEL = 5


def main():
    """Densidade de estações por célula H3 nível 5."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("inmet-estacoes-hexbin-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    geo = spark.read.format("geoparquet").load(ORIGEM)

    indexed = dt.attach_h3_index(geo, "geometry", H3_LEVEL, "h3_cell")

    hexbin = dt.aggregate_by_h3(
        indexed,
        "h3_cell",
        [
            F.count(F.lit(1)).alias("estacoes"),
            F.round(F.avg(F.col("altitude")), 1).alias("altitude_media"),
        ],
    )

    hexbin.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold inmet_estacoes_hexbin: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
