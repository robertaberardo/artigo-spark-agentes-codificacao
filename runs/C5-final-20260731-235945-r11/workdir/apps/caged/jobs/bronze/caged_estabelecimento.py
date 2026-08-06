from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("caged", "estabelecimento", "raw")
DESTINO = table_location("caged", "estabelecimento", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("cnpj", StringType()),
        StructField("uf", StringType()),
        StructField("municipio_ibge", StringType()),
        StructField("cnae", StringType()),
        StructField("porte", StringType()),
    ]
)


def main():
    """Lê o raw dos estabelecimentos, normaliza os códigos e salva a bronze."""
    spark = SparkSession.builder.appName("caged-estabelecimento-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(raw["cnpj"], 14).alias("cnpj"),
        ct.standardize_uf(raw["uf"]).alias("uf"),
        ct.zero_pad_code(raw["municipio_ibge"], 7).alias("municipio_ibge"),
        ct.zero_pad_code(raw["cnae"], 7).alias("cnae"),
        ct.normalize_text(raw["porte"]).alias("porte"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze caged_estabelecimento: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
