from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("datasus", "obito", "raw")
COBERTURA = table_location("datasus", "municipio_cobertura", "bronze")
DESTINO = table_location("datasus", "obito_consolidado", "silver")

RAW_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("municipio_ibge", StringType()),
        StructField("uf", StringType()),
        StructField("data_obito", StringType()),
        StructField("sexo", StringType()),
        StructField("idade", StringType()),
        StructField("causa_cid", StringType()),
    ]
)


def main():
    """Consolida óbitos por município com a população e a taxa por 100 mil habitantes."""
    spark = SparkSession.builder.appName("datasus-obito-consolidado-silver").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    obito = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )
    cobertura = spark.read.parquet(COBERTURA).select(
        F.col("municipio_ibge"), F.col("populacao")
    )

    por_municipio = (
        obito.select(
            ct.digits_only(F.col("municipio_ibge")).alias("municipio_ibge"),
            ct.standardize_uf(F.col("uf")).alias("uf"),
        )
        .groupBy("municipio_ibge", "uf")
        .agg(F.count(F.lit(1)).cast("long").alias("obitos"))
    )

    consolidado = (
        por_municipio.transform(
            lambda d: dt.join_lookup(d, cobertura, "municipio_ibge")
        )
        .withColumn(
            "taxa_100k",
            ct.safe_divide(F.col("obitos"), F.col("populacao")) * F.lit(100000.0),
        )
        .select("municipio_ibge", "uf", "obitos", "populacao", "taxa_100k")
    )

    consolidado.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> silver datasus_obito_consolidado: {n} municípios em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
