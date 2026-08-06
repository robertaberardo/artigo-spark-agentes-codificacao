from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.anac.utils import column_transforms as anac_ct
from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("anac", "aerodromo", "raw")
DESTINO = table_location("anac", "aerodromo", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("codigo_oaci", StringType()),
        StructField("nome", StringType()),
        StructField("municipio", StringType()),
        StructField("uf", StringType()),
        StructField("tipo", StringType()),
        StructField("latitude", StringType()),
        StructField("longitude", StringType()),
        StructField("altitude_m", StringType()),
    ]
)


def main():
    """Lê o raw dos aeródromos, aplica as utils de coluna e salva a bronze."""
    spark = SparkSession.builder.appName("anac-aerodromo-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.upper(F.trim(F.col("codigo_oaci"))).alias("codigo_oaci"),
        ct.normalize_text(F.col("nome")).alias("nome"),
        ct.normalize_text(F.col("municipio")).alias("municipio"),
        ct.standardize_uf(F.col("uf")).alias("uf"),
        ct.blank_to_null(F.col("tipo")).alias("tipo"),
        ct.to_coordinate(F.col("latitude")).alias("latitude"),
        ct.to_coordinate(F.col("longitude")).alias("longitude"),
        ct.br_decimal_to_double(F.col("altitude_m")).alias("altitude_m"),
    ).filter(anac_ct.is_valid_oaci(F.col("codigo_oaci")))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze anac_aerodromo: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
