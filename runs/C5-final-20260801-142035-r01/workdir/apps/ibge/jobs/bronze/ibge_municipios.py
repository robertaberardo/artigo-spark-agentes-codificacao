from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.ibge.utils import column_transforms as ibge_ct
from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("ibge", "municipios", "raw")
DESTINO = table_location("ibge", "municipios", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("code", StringType()),
        StructField("name", StringType()),
        StructField("uf", StringType()),
        StructField("area_km2", StringType()),
        StructField("lat", StringType()),
        StructField("lon", StringType()),
    ]
)


def main():
    """Lê o raw dos municípios, aplica as utils de coluna e salva a bronze."""
    spark = (
        SparkSession.builder.appName("ibge-municipios-bronze").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.blank_to_null(F.col("code")).alias("code"),
        ct.normalize_text(F.col("name")).alias("name"),
        ct.blank_to_null(F.col("uf")).alias("uf"),
        ct.br_decimal_to_double(F.col("area_km2")).alias("area_km2"),
        ct.to_coordinate(F.col("lat")).alias("lat"),
        ct.to_coordinate(F.col("lon")).alias("lon"),
    ).filter(ibge_ct.is_valid_municipio_code(F.col("code")))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze ibge_municipios: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
