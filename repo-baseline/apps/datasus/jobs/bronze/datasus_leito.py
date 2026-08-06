from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("datasus", "leito", "raw")
DESTINO = table_location("datasus", "leito", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("co_cnes", StringType()),
        StructField("tipo_leito", StringType()),
        StructField("quantidade", StringType()),
        StructField("sus", StringType()),
    ]
)


def main():
    """Lê o raw dos leitos, tipa quantidade e indicador SUS e salva a bronze."""
    spark = SparkSession.builder.appName("datasus-leito-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(F.col("co_cnes"), 7).alias("co_cnes"),
        ct.normalize_text(F.col("tipo_leito")).alias("tipo_leito"),
        ct.coalesce_zero(F.col("quantidade").cast("int")).alias("quantidade"),
        ct.yes_no_to_int(F.col("sus")).alias("sus"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze datasus_leito: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
