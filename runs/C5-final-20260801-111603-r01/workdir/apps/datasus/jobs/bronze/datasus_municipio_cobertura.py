from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("datasus", "municipio_cobertura", "raw")
DESTINO = table_location("datasus", "municipio_cobertura", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("municipio_ibge", StringType()),
        StructField("uf", StringType()),
        StructField("populacao", StringType()),
        StructField("cobertura_aps_pct", StringType()),
    ]
)


def main():
    """Lê o raw da cobertura por município, tipa população e cobertura, salva a bronze."""
    spark = SparkSession.builder.appName("datasus-municipio-cobertura-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.digits_only(F.col("municipio_ibge")).alias("municipio_ibge"),
        ct.standardize_uf(F.col("uf")).alias("uf"),
        F.col("populacao").cast("long").alias("populacao"),
        ct.percent_to_fraction(F.col("cobertura_aps_pct")).alias("cobertura_aps"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze datasus_municipio_cobertura: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
