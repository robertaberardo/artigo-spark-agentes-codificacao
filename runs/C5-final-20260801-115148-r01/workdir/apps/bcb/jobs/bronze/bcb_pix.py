from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("bcb", "pix", "raw")
DESTINO = table_location("bcb", "pix", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("data", StringType()),
        StructField("uf", StringType()),
        StructField("quantidade", StringType()),
        StructField("valor_total", StringType()),
    ]
)


def main():
    """Lê o raw das estatísticas do Pix, tipa as colunas e salva a bronze."""
    spark = SparkSession.builder.appName("bcb-pix-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.parse_br_date(F.col("data")).alias("data"),
        ct.standardize_uf(F.col("uf")).alias("uf"),
        F.col("quantidade").cast("long").alias("quantidade"),
        ct.money_br_to_double(F.col("valor_total")).alias("valor_total"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze bcb_pix: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
