from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.ana.utils import column_transforms as ana_ct
from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("ana", "estacao", "raw")
DESTINO = table_location("ana", "estacao", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("codigo", StringType()),
        StructField("nome", StringType()),
        StructField("rio", StringType()),
        StructField("uf", StringType()),
        StructField("latitude", StringType()),
        StructField("longitude", StringType()),
        StructField("tipo", StringType()),
    ]
)


def main():
    """Lê o raw das estações, aplica as utils de coluna e salva a bronze."""
    spark = SparkSession.builder.appName("ana-estacao-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.upper(F.trim(F.col("codigo"))).alias("codigo"),
        ct.normalize_text(F.col("nome")).alias("nome"),
        ct.normalize_text(F.col("rio")).alias("rio"),
        ct.standardize_uf(F.col("uf")).alias("uf"),
        ct.to_coordinate(F.col("latitude")).alias("latitude"),
        ct.to_coordinate(F.col("longitude")).alias("longitude"),
        ct.blank_to_null(F.col("tipo")).alias("tipo"),
    ).filter(ana_ct.is_valid_station_code(F.col("codigo")))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze ana_estacao: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
