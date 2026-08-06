from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.inmet.utils import column_transforms as inmet_ct
from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("inmet", "estacao", "raw")
DESTINO = table_location("inmet", "estacao", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("codigo_wmo", StringType()),
        StructField("nome", StringType()),
        StructField("uf", StringType()),
        StructField("latitude", StringType()),
        StructField("longitude", StringType()),
        StructField("altitude", StringType()),
    ]
)


def main():
    """Lê o raw das estações, aplica as utils de coluna e salva a bronze."""
    spark = SparkSession.builder.appName("inmet-estacao-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.upper(F.trim(F.col("codigo_wmo"))).alias("codigo_wmo"),
        ct.normalize_text(F.col("nome")).alias("nome"),
        ct.standardize_uf(F.col("uf")).alias("uf"),
        ct.to_coordinate(F.col("latitude")).alias("latitude"),
        ct.to_coordinate(F.col("longitude")).alias("longitude"),
        ct.br_decimal_to_double(F.col("altitude")).alias("altitude"),
    ).filter(inmet_ct.is_valid_wmo_code(F.col("codigo_wmo")))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze inmet_estacao: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
