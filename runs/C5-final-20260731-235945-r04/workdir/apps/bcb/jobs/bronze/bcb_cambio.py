from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.bcb.utils import column_transforms as bcb_ct
from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("bcb", "cambio", "raw")
DESTINO = table_location("bcb", "cambio", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("data", StringType()),
        StructField("moeda", StringType()),
        StructField("compra", StringType()),
        StructField("venda", StringType()),
    ]
)


def main():
    """Lê o raw das cotações de câmbio, tipa as colunas e salva a bronze."""
    spark = SparkSession.builder.appName("bcb-cambio-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.parse_br_date(F.col("data")).alias("data"),
        bcb_ct.normalize_currency(F.col("moeda")).alias("moeda"),
        ct.br_decimal_to_double(F.col("compra")).alias("compra"),
        ct.br_decimal_to_double(F.col("venda")).alias("venda"),
    ).filter(bcb_ct.is_valid_currency_code(F.col("moeda")))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze bcb_cambio: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
