from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct

ORIGEM = "s3a://datalake/inmet/inmet_normal_climatologica/raw"
DESTINO = "s3a://datalake/inmet/inmet_normal_climatologica/bronze"

RAW_SCHEMA = StructType(
    [
        StructField("codigo_estacao", StringType()),
        StructField("mes", StringType()),
        StructField("temp_media", StringType()),
        StructField("precip_media", StringType()),
    ]
)


def main():
    """Lê o raw das normais climatológicas, tipa as colunas e salva a bronze."""
    spark = SparkSession.builder.appName("inmet-normal-climatologica-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.upper(F.trim(F.col("codigo_estacao"))).alias("codigo_estacao"),
        ct.blank_to_null(F.col("mes")).cast("int").alias("mes"),
        ct.br_decimal_to_double(F.col("temp_media")).alias("temp_media"),
        ct.br_decimal_to_double(F.col("precip_media")).alias("precip_media"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze inmet_normal_climatologica: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
