from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct

ORIGEM = "s3a://datalake/cvm/cvm_informe_diario/raw"
DESTINO = "s3a://datalake/cvm/cvm_informe_diario/bronze"

RAW_SCHEMA = StructType(
    [
        StructField("cnpj_fundo", StringType()),
        StructField("data", StringType()),
        StructField("captacao", StringType()),
        StructField("resgate", StringType()),
    ]
)


def main():
    """Lê o raw dos informes diários, converte os valores em reais e salva a bronze."""
    spark = SparkSession.builder.appName("cvm-informe-diario-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.upper(F.trim(F.col("cnpj_fundo"))).alias("cnpj_fundo"),
        ct.parse_br_date(F.col("data")).alias("data"),
        ct.money_br_to_double(F.col("captacao")).alias("captacao"),
        ct.money_br_to_double(F.col("resgate")).alias("resgate"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze cvm_informe_diario: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
