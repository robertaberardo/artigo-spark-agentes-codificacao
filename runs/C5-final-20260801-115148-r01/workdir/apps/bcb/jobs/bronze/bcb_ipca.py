from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("bcb", "ipca", "raw")
DESTINO = table_location("bcb", "ipca", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("data", StringType()),
        StructField("indice", StringType()),
        StructField("variacao_mensal_pct", StringType()),
    ]
)


def main():
    """Lê o raw do IPCA, tipa as colunas e salva a bronze."""
    spark = SparkSession.builder.appName("bcb-ipca-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.parse_br_date(F.col("data")).alias("data"),
        ct.br_decimal_to_double(F.col("indice")).alias("indice"),
        ct.percent_to_fraction(F.col("variacao_mensal_pct")).alias(
            "variacao_mensal_pct"
        ),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze bcb_ipca: {n} registros em {DESTINO}")
    spark.stop()


main()
