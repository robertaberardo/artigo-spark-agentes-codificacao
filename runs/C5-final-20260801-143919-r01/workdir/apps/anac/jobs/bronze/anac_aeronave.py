from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("anac", "aeronave", "raw")
DESTINO = table_location("anac", "aeronave", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("matricula", StringType()),
        StructField("empresa_icao", StringType()),
        StructField("modelo", StringType()),
        StructField("fabricante", StringType()),
        StructField("ano_fabricacao", StringType()),
        StructField("assentos", StringType()),
    ]
)


def main():
    """Lê o raw da frota, seleciona e tipa as colunas de interesse na bronze."""
    spark = SparkSession.builder.appName("anac-aeronave-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.upper(F.trim(F.col("matricula"))).alias("matricula"),
        ct.blank_to_null(F.col("empresa_icao")).alias("empresa_icao"),
        ct.normalize_text(F.col("modelo")).alias("modelo"),
        ct.normalize_text(F.col("fabricante")).alias("fabricante"),
        ct.blank_to_null(F.col("ano_fabricacao")).cast("int").alias("ano_fabricacao"),
        ct.blank_to_null(F.col("assentos")).cast("int").alias("assentos"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze anac_aeronave: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
