from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils import column_transforms as ct
from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("bcb", "pix", "bronze")
DESTINO = table_location("bcb", "pix_consolidado", "silver")


def main():
    """Consolida o Pix por (data, UF), mantendo o registro mais recente e o ticket."""
    spark = SparkSession.builder.appName("bcb-pix-consolidado-silver").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    pix = spark.read.parquet(ORIGEM)

    result = (
        pix.transform(
            lambda d: dt.dedupe_keep_latest(d, ["data", "uf"], "valor_total")
        )
        .withColumn("quantidade", ct.coalesce_zero(F.col("quantidade")))
        .withColumn(
            "ticket_medio",
            ct.safe_divide(F.col("valor_total"), F.col("quantidade")),
        )
        .select(
            "data",
            "uf",
            "quantidade",
            "valor_total",
            "ticket_medio",
        )
    )

    result.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> silver bcb_pix_consolidado: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
