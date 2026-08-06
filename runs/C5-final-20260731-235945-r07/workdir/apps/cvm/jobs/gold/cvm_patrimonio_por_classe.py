from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from utils import dataframe_transforms as dt
from utils.metadata import table_location

FUNDO = table_location("cvm", "fundo_consolidado", "silver")
COTA_SERIE = table_location("cvm", "cota_serie", "silver")
DESTINO = table_location("cvm", "patrimonio_por_classe", "gold")


def main():
    """Agrega o patrimônio líquido por classe de fundo e sua participação no total."""
    spark = SparkSession.builder.appName("cvm-patrimonio-por-classe-gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    fundo = spark.read.parquet(FUNDO)
    cota = spark.read.parquet(COTA_SERIE)

    ordem = Window.partitionBy(cota["cnpj_fundo"]).orderBy(cota["data"].desc())
    cota = cota.withColumn("_rn", F.row_number().over(ordem))
    ultimo = cota.filter(cota["_rn"] == 1)
    ultimo = ultimo.select(
        cota["cnpj_fundo"].alias("cnpj"),
        cota["patrimonio_liquido"].alias("patrimonio_liquido"),
    )

    fundo = fundo.select(fundo["cnpj"], fundo["classe"])
    juntado = dt.join_no_fanout(fundo, ultimo, on="cnpj", how="inner")

    por_classe = juntado.groupBy("classe").agg(
        F.count(F.lit(1)).cast("long").alias("fundos"),
        F.sum(juntado["patrimonio_liquido"]).alias("patrimonio_total"),
    )

    total = Window.partitionBy()
    por_classe = por_classe.withColumn(
        "participacao",
        por_classe["patrimonio_total"] / F.sum(por_classe["patrimonio_total"]).over(total),
    )

    resultado = por_classe.select(
        "classe",
        "fundos",
        "patrimonio_total",
        "participacao",
    ).orderBy(F.col("patrimonio_total").desc())

    resultado.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> gold cvm_patrimonio_por_classe: {n} classes em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
