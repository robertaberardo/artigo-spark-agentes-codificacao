from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.dnit.utils import column_transforms as dnit_ct
from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("dnit", "pnv", "raw")
DESTINO = table_location("dnit", "pnv", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("codigo_pnv", StringType()),
        StructField("br", StringType()),
        StructField("uf", StringType()),
        StructField("superficie", StringType()),
        StructField("condicao", StringType()),
    ]
)


def main():
    """Lê o raw do PNV, normaliza superfície e condição e salva a bronze."""
    spark = SparkSession.builder.appName("dnit-pnv-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.upper(F.trim(F.col("codigo_pnv"))).alias("codigo_pnv"),
        dnit_ct.normalize_br(F.col("br")).alias("br"),
        ct.standardize_uf(F.col("uf")).alias("uf"),
        ct.strip_accents_lower(F.col("superficie")).alias("superficie"),
        ct.strip_accents_lower(F.col("condicao")).alias("condicao"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze dnit_pnv: {n} registros em {DESTINO}")
    spark.stop()


main()
