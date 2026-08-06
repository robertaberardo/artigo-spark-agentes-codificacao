from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.inmet.utils import column_transforms as inmet_ct
from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("inmet", "medicao", "raw")
DESTINO = table_location("inmet", "medicao", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("codigo_estacao", StringType()),
        StructField("data_hora", StringType()),
        StructField("temp_c", StringType()),
        StructField("umidade_pct", StringType()),
        StructField("precip_mm", StringType()),
        StructField("vento_ms", StringType()),
    ]
)


def main():
    """Lê o raw das medições, tipa as colunas de interesse e salva a bronze."""
    spark = SparkSession.builder.appName("inmet-medicao-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    temp_c = F.col("temp_c").cast("double")

    bronze = raw.select(
        F.upper(F.trim(F.col("codigo_estacao"))).alias("codigo_estacao"),
        ct.parse_iso_timestamp(F.col("data_hora")).alias("data_hora"),
        F.when(inmet_ct.celsius_is_plausible(temp_c), temp_c).alias("temp_c"),
        F.col("umidade_pct").cast("double").alias("umidade_pct"),
        ct.null_if_negative(F.col("precip_mm").cast("double")).alias("precip_mm"),
        F.col("vento_ms").cast("double").alias("vento_ms"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze inmet_medicao: {n} registros em {DESTINO}")
    spark.stop()


main()
