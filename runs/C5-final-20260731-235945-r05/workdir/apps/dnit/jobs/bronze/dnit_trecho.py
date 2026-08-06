from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.dnit.utils import column_transforms as dnit_ct
from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("dnit", "trecho", "raw")
DESTINO = table_location("dnit", "trecho", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("id_trecho", StringType()),
        StructField("br", StringType()),
        StructField("uf", StringType()),
        StructField("km_inicial", StringType()),
        StructField("km_final", StringType()),
        StructField("tipo_pista", StringType()),
    ]
)


def main():
    """Lê o raw dos trechos, tipa os quilômetros e salva a bronze."""
    spark = SparkSession.builder.appName("dnit-trecho-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.normalize_text(F.col("id_trecho")).alias("id_trecho"),
        dnit_ct.normalize_br(F.col("br")).alias("br"),
        ct.standardize_uf(F.col("uf")).alias("uf"),
        ct.br_decimal_to_double(F.col("km_inicial")).alias("km_inicial"),
        ct.br_decimal_to_double(F.col("km_final")).alias("km_final"),
        ct.normalize_text(F.col("tipo_pista")).alias("tipo_pista"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze dnit_trecho: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
