from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.cvm.utils import column_transforms as cvm_ct
from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("cvm", "fundo", "raw")
DESTINO = table_location("cvm", "fundo", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("cnpj", StringType()),
        StructField("nome", StringType()),
        StructField("classe", StringType()),
        StructField("situacao", StringType()),
        StructField("data_registro", StringType()),
    ]
)


def main():
    """Lê o raw do cadastro de fundos, tipa as colunas e salva a bronze."""
    spark = SparkSession.builder.appName("cvm-fundo-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.upper(F.trim(F.col("cnpj"))).alias("cnpj"),
        ct.normalize_text(F.col("nome")).alias("nome"),
        ct.normalize_text(F.col("classe")).alias("classe"),
        ct.blank_to_null(F.col("situacao")).alias("situacao"),
        ct.parse_br_date(F.col("data_registro")).alias("data_registro"),
    ).filter(cvm_ct.is_valid_cnpj(F.col("cnpj")))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze cvm_fundo: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
