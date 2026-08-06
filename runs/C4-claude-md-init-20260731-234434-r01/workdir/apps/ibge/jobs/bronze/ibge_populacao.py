from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("ibge", "populacao", "raw")
DESTINO = table_location("ibge", "populacao", "bronze")


RAW_SCHEMA = StructType(
    [
        StructField("code", StringType()),
        StructField("populacao", StringType()),
    ]
)


def main():
    """Lê o raw da população, normaliza código/valor e salva a bronze."""
    spark = (
        SparkSession.builder.appName("ibge-populacao-bronze").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.blank_to_null(F.col("code")).alias("code"),
        ct.blank_to_null(F.col("populacao")).cast("long").alias("populacao"),
    ).filter(F.col("code").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze ibge_populacao: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
