from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("cvm", "administrador", "raw")
DESTINO = table_location("cvm", "administrador", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("cnpj", StringType()),
        StructField("nome", StringType()),
        StructField("tipo", StringType()),
    ]
)


def main():
    """Lê o raw dos administradores, padroniza os textos e salva a bronze."""
    spark = SparkSession.builder.appName("cvm-administrador-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.upper(F.trim(raw["cnpj"])).alias("cnpj"),
        ct.normalize_text(raw["nome"]).alias("nome"),
        ct.blank_to_null(raw["tipo"]).alias("tipo"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze cvm_administrador: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
