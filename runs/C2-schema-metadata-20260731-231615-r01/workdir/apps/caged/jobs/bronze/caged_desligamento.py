from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("caged", "desligamento", "raw")
DESTINO = table_location("caged", "desligamento", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("municipio_ibge", StringType()),
        StructField("uf", StringType()),
        StructField("cbo", StringType()),
        StructField("cnae", StringType()),
        StructField("data", StringType()),
        StructField("salario", StringType()),
        StructField("motivo", StringType()),
    ]
)


def main():
    """Lê o raw dos desligamentos, tipa as colunas e salva a bronze."""
    spark = SparkSession.builder.appName("caged-desligamento-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.trim(F.col("id")).alias("id"),
        ct.zero_pad_code(F.col("municipio_ibge"), 7).alias("municipio_ibge"),
        ct.standardize_uf(F.col("uf")).alias("uf"),
        ct.zero_pad_code(F.col("cbo"), 6).alias("cbo"),
        ct.zero_pad_code(F.col("cnae"), 7).alias("cnae"),
        ct.parse_br_date(F.col("data")).alias("data"),
        ct.money_br_to_double(F.col("salario")).alias("salario"),
        ct.normalize_text(F.col("motivo")).alias("motivo"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze caged_desligamento: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
