from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("dnit", "acidente_geo", "silver")
DESTINO = table_location("dnit", "segmentos_criticos", "gold")

H3_LEVEL = 7


def main():
    """Concentração de acidentes por célula H3 nível 7 (segmentos críticos)."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("dnit-segmentos-criticos-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    acidentes = spark.read.format("geoparquet").load(ORIGEM)

    criticos = acidentes.transform(
        lambda d: dt.attach_h3_index(d, "geometry", H3_LEVEL, "h3_cell")
    ).transform(
        lambda d: dt.aggregate_by_h3(
            d,
            "h3_cell",
            [
                F.count(F.lit(1)).cast("long").alias("acidentes"),
                F.sum("mortos").cast("long").alias("mortos_total"),
            ],
        )
    )

    criticos.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold dnit_segmentos_criticos: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
