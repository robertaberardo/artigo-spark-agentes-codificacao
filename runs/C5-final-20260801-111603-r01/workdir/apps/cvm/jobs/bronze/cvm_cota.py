from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("cvm", "cota", "raw")
DESTINO = table_location("cvm", "cota", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("cnpj_fundo", StringType()),
        StructField("data", StringType()),
        StructField("valor_cota", StringType()),
        StructField("patrimonio_liquido", StringType()),
        StructField("cotistas", StringType()),
    ]
)


def main():
    """Lê o raw das cotas diárias, tipa as colunas de interesse e salva a bronze."""
    spark = SparkSession.builder.appName("cvm-cota-bronze").getOrCreate()
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
        F.col("valor_cota").cast("double").alias("valor_cota"),
        ct.money_br_to_double(F.col("patrimonio_liquido")).alias("patrimonio_liquido"),
        F.col("cotistas").cast("long").alias("cotistas"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze cvm_cota: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
