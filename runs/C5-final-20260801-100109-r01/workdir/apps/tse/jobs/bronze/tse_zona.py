from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("tse", "zona", "raw")
DESTINO = table_location("tse", "zona", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("id_zona", StringType()),
        StructField("municipio_ibge", StringType()),
        StructField("uf", StringType()),
        StructField("latitude", StringType()),
        StructField("longitude", StringType()),
        StructField("eleitores", StringType()),
    ]
)


def main():
    """Lê o raw das zonas eleitorais, tipa coordenadas e eleitorado na bronze."""
    spark = SparkSession.builder.appName("tse-zona-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.upper(F.trim(F.col("id_zona"))).alias("id_zona"),
        ct.zero_pad_code(F.col("municipio_ibge"), 7).alias("municipio_ibge"),
        ct.standardize_uf(F.col("uf")).alias("uf"),
        ct.to_coordinate(F.col("latitude")).alias("latitude"),
        ct.to_coordinate(F.col("longitude")).alias("longitude"),
        ct.blank_to_null(F.col("eleitores")).cast("int").alias("eleitores"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze tse_zona: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
