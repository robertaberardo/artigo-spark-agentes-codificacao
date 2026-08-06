from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("datasus", "estabelecimento_geo", "silver")
DESTINO = table_location("datasus", "estabelecimento_hexbin", "gold")

H3_LEVEL = 6


def main():
    """Densidade de estabelecimentos por célula H3 nível 6."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("datasus-estabelecimento-hexbin-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    geo = spark.read.format("geoparquet").load(ORIGEM)

    hexbin = geo.transform(
        lambda d: dt.attach_h3_index(d, "geometry", H3_LEVEL, "h3_cell")
    ).transform(
        lambda d: dt.aggregate_by_h3(
            d,
            "h3_cell",
            [
                F.count(F.lit(1)).cast("long").alias("estabelecimentos"),
                F.sum("leitos_sus").cast("long").alias("leitos_sus_total"),
            ],
        )
    )

    hexbin.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold datasus_estabelecimento_hexbin: {n} células em {DESTINO}")
    spark.stop()


main()
