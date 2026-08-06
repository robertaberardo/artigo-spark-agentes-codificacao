from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

from utils.metadata import table_location

ORIGEM = table_location("snis", "prestador", "bronze")
DESTINO = table_location("snis", "prestador_consolidado", "silver")


def main():
    """Consolida o cadastro de prestadores: padroniza os campos e remove duplicados."""
    spark = SparkSession.builder.appName(
        "snis-prestador-consolidado-silver"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    prestador = spark.read.parquet(ORIGEM)

    prestador = prestador.withColumn("codigo", F.upper(F.trim(F.col("codigo"))))
    prestador = prestador.withColumn(
        "nome", F.trim(F.regexp_replace(F.col("nome"), r"\s+", " "))
    )
    prestador = prestador.withColumn("uf", F.upper(F.trim(F.col("uf"))))
    prestador = prestador.withColumn(
        "natureza_juridica", F.upper(F.trim(F.col("natureza_juridica")))
    )

    window = Window.partitionBy("codigo").orderBy(F.col("nome").asc())
    prestador = prestador.withColumn("_rn", F.row_number().over(window))
    prestador = prestador.filter(F.col("_rn") == 1)
    prestador = prestador.drop("_rn")

    result = prestador.select(
        "codigo", "nome", "uf", "natureza_juridica", "abrangencia"
    )

    result.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> silver snis_prestador_consolidado: {n} prestadores em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
