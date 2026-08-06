from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("ana", "medicao_fluvio", "raw")
DESTINO = table_location("ana", "medicao_fluvio", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("codigo_estacao", StringType()),
        StructField("data", StringType()),
        StructField("nivel_cm", StringType()),
        StructField("vazao_m3s", StringType()),
        StructField("chuva_mm", StringType()),
    ]
)


def main():
    """Lê o raw das medições, converte a data e tipa as medidas na bronze."""
    spark = SparkSession.builder.appName("ana-medicao-fluvio-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.upper(F.trim(F.col("codigo_estacao").cast("string"))).alias("codigo_estacao"),
        ct.parse_br_date(F.col("data").cast("string")).alias("data"),
        F.col("nivel_cm").cast("double").alias("nivel_cm"),
        F.col("vazao_m3s").cast("double").alias("vazao_m3s"),
        F.col("chuva_mm").cast("double").alias("chuva_mm"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze ana_medicao_fluvio: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
