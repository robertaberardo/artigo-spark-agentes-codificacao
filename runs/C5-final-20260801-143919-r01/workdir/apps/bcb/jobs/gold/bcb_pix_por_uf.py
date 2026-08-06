from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils import column_transforms as ct
from utils import dataframe_transforms as dt

ORIGEM = "s3a://datalake/bcb/bcb_pix_consolidado/silver"
DESTINO = "s3a://datalake/bcb/bcb_pix_por_uf/gold"


def main():
    """Agrega os totais do Pix por UF, com ticket médio, participação e ranking."""
    spark = SparkSession.builder.appName("bcb-pix-por-uf-gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    pix = spark.read.parquet(ORIGEM)

    totais = pix.groupBy("uf").agg(
        F.sum("quantidade").cast("long").alias("total_quantidade"),
        F.sum("valor_total").alias("total_valor"),
    )

    result = (
        totais.withColumn(
            "ticket_medio",
            ct.safe_divide(F.col("total_valor"), F.col("total_quantidade")),
        )
        .transform(
            lambda d: dt.add_group_share(d, "total_valor", [], out_col="share_valor")
        )
        .transform(
            lambda d: dt.rank_within_group(
                d, partition_cols=[], order_col="total_valor", out_col="rank_valor"
            )
        )
        .select(
            "uf",
            "total_quantidade",
            "total_valor",
            "ticket_medio",
            "share_valor",
            "rank_valor",
        )
        .orderBy(F.col("rank_valor").asc())
    )

    result.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> gold bcb_pix_por_uf: {n} UFs em {DESTINO}")
    result.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
