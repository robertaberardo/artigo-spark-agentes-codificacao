from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from utils import dataframe_transforms as dt
from utils.metadata import table_location

FUNDO = table_location("cvm", "fundo", "bronze")
ADMINISTRADOR = table_location("cvm", "administrador", "bronze")
DESTINO = table_location("cvm", "fundo_consolidado", "silver")


def main():
    """Consolida o cadastro de fundos com os dados do administrador correspondente."""
    spark = SparkSession.builder.appName("cvm-fundo-consolidado-silver").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    fundo = spark.read.parquet(FUNDO)
    administrador = spark.read.parquet(ADMINISTRADOR)

    ordem_fundo = Window.partitionBy("cnpj").orderBy(F.col("data_registro").desc())
    fundo = fundo.withColumn("_rn", F.row_number().over(ordem_fundo))
    fundo = fundo.filter(F.col("_rn") == 1).drop("_rn")

    admin = administrador.groupBy("cnpj").agg(
        F.first("nome", ignorenulls=True).alias("admin_nome"),
        F.first("tipo", ignorenulls=True).alias("admin_tipo"),
    )

    consolidado = dt.join_no_fanout(fundo, admin, on="cnpj", how="left")
    consolidado = consolidado.select(
        "cnpj",
        "nome",
        "classe",
        "situacao",
        "data_registro",
        "admin_nome",
        "admin_tipo",
    )

    consolidado.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> silver cvm_fundo_consolidado: {n} fundos em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
