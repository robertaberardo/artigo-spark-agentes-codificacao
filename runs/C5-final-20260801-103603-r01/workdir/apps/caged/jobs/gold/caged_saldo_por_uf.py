from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils import column_transforms as ct
from utils import dataframe_transforms as dt
from utils.metadata import table_location

ADMISSAO = table_location("caged", "admissao", "bronze")
DESLIGAMENTO = table_location("caged", "desligamento", "bronze")
DESTINO = table_location("caged", "saldo_por_uf", "gold")


def main():
    """Saldo de empregos por UF: admissões menos desligamentos."""
    spark = SparkSession.builder.appName("caged-saldo-por-uf-gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    admissoes = (
        spark.read.parquet(ADMISSAO)
        .groupBy("uf")
        .agg(F.count(F.lit(1)).cast("long").alias("admissions"))
    )
    desligamentos = (
        spark.read.parquet(DESLIGAMENTO)
        .groupBy("uf")
        .agg(F.count(F.lit(1)).cast("long").alias("terminations"))
    )

    gold = (
        dt.join_no_fanout(admissoes, desligamentos, on="uf", how="outer")
        .withColumn("admissions", ct.coalesce_zero(F.col("admissions")).cast("long"))
        .withColumn("terminations", ct.coalesce_zero(F.col("terminations")).cast("long"))
        .withColumn(
            "net_balance", (F.col("admissions") - F.col("terminations")).cast("long")
        )
        .orderBy(F.col("net_balance").desc())
    )

    gold.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> gold caged_saldo_por_uf: {n} UFs em {DESTINO}")
    gold.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
