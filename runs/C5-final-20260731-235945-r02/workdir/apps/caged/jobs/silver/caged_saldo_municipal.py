from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ADMISSAO = table_location("caged", "admissao", "bronze")
DESLIGAMENTO = table_location("caged", "desligamento", "bronze")
DESTINO = table_location("caged", "saldo_municipal", "silver")

GRUPO = ["municipio_ibge", "uf"]


def main():
    """Saldo de empregos por município: admissões menos desligamentos."""
    spark = SparkSession.builder.appName("caged-saldo-municipal-silver").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    admissao = spark.read.parquet(ADMISSAO)
    desligamento = spark.read.parquet(DESLIGAMENTO)

    admissoes = admissao.groupBy(GRUPO).agg(
        F.count(F.lit(1)).cast("long").alias("admissions")
    )
    desligamentos = desligamento.groupBy(GRUPO).agg(
        F.count(F.lit(1)).cast("long").alias("terminations")
    )

    saldo = dt.join_no_fanout(admissoes, desligamentos, on=GRUPO, how="outer")
    saldo = saldo.withColumn(
        "admissions", F.coalesce(F.col("admissions"), F.lit(0)).cast("long")
    )
    saldo = saldo.withColumn(
        "terminations", F.coalesce(F.col("terminations"), F.lit(0)).cast("long")
    )
    saldo = saldo.withColumn(
        "net_balance", (F.col("admissions") - F.col("terminations")).cast("long")
    )

    silver = saldo.select(
        "municipio_ibge", "uf", "admissions", "terminations", "net_balance"
    )

    silver.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> silver caged_saldo_municipal: {n} municípios em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
