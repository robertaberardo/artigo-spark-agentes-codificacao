from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct

ORIGEM = "s3a://datalake/snis/snis_indicador_esgoto/raw"
DESTINO = "s3a://datalake/snis/snis_indicador_esgoto/bronze"

RAW_SCHEMA = StructType(
    [
        StructField("municipio_ibge", StringType()),
        StructField("ano", StringType()),
        StructField("indice_coleta_esgoto_pct", StringType()),
        StructField("indice_tratamento_esgoto_pct", StringType()),
    ]
)


def main():
    """Lê o raw dos indicadores de esgoto, converte percentuais e salva a bronze."""
    spark = SparkSession.builder.appName("snis-indicador-esgoto-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(F.col("municipio_ibge"), 7).alias("municipio_ibge"),
        F.col("ano").cast("int").alias("ano"),
        ct.percent_to_fraction(F.col("indice_coleta_esgoto_pct")).alias(
            "indice_coleta_esgoto_pct"
        ),
        ct.percent_to_fraction(F.col("indice_tratamento_esgoto_pct")).alias(
            "indice_tratamento_esgoto_pct"
        ),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze snis_indicador_esgoto: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
