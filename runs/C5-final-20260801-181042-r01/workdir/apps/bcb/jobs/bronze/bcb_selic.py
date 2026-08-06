from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("bcb", "selic", "raw")
DESTINO = table_location("bcb", "selic", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("data", StringType()),
        StructField("taxa_meta_pct", StringType()),
        StructField("taxa_efetiva_pct", StringType()),
    ]
)


def main():
    """Lê o raw da taxa Selic, tipa as colunas e salva a bronze."""
    spark = SparkSession.builder.appName("bcb-selic-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.parse_br_date(raw["data"]).alias("data"),
        ct.br_decimal_to_double(raw["taxa_meta_pct"]).alias("taxa_meta_pct"),
        ct.clamp(ct.br_decimal_to_double(raw["taxa_efetiva_pct"]), 0.0, 100.0).alias(
            "taxa_efetiva_pct"
        ),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze bcb_selic: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
