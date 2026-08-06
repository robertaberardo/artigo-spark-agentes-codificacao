from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("anac", "empresa", "raw")
DESTINO = table_location("anac", "empresa", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("icao", StringType()),
        StructField("nome", StringType()),
        StructField("uf", StringType()),
        StructField("ativa", StringType()),
    ]
)


def main():
    """Lê o raw das empresas aéreas, tipa e salva a bronze."""
    spark = SparkSession.builder.appName("anac-empresa-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.upper(F.trim(F.col("icao"))).alias("icao"),
        ct.normalize_text(F.col("nome")).alias("nome"),
        ct.standardize_uf(F.col("uf")).alias("uf"),
        ct.yes_no_to_int(F.col("ativa")).alias("ativa"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze anac_empresa: {n} registros em {DESTINO}")
    spark.stop()


main()
