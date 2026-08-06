from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("caged", "cbo", "raw")
DESTINO = table_location("caged", "cbo", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("codigo", StringType()),
        StructField("descricao", StringType()),
        StructField("familia", StringType()),
    ]
)


def main():
    """Lê o raw das ocupações CBO, padroniza o texto e salva a bronze."""
    spark = SparkSession.builder.appName("caged-cbo-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(F.col("codigo"), 6).alias("codigo"),
        ct.normalize_text(F.col("descricao")).alias("descricao"),
        ct.normalize_text(F.col("familia")).alias("familia"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze caged_cbo: {n} registros em {DESTINO}")
    spark.stop()


main()
