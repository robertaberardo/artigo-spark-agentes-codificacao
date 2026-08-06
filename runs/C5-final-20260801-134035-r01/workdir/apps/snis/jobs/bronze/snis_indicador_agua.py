from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("snis", "indicador_agua", "raw")
DESTINO = table_location("snis", "indicador_agua", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("municipio_ibge", StringType()),
        StructField("ano", StringType()),
        StructField("indice_atendimento_agua_pct", StringType()),
        StructField("consumo_medio_l_hab_dia", StringType()),
        StructField("perda_faturamento_pct", StringType()),
    ]
)


def main():
    """Lê o raw dos indicadores de água, converte percentuais e salva a bronze."""
    spark = SparkSession.builder.appName("snis-indicador-agua-bronze").getOrCreate()
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
        ct.percent_to_fraction(F.col("indice_atendimento_agua_pct")).alias(
            "indice_atendimento_agua_pct"
        ),
        ct.br_decimal_to_double(F.col("consumo_medio_l_hab_dia")).alias(
            "consumo_medio_l_hab_dia"
        ),
        ct.percent_to_fraction(F.col("perda_faturamento_pct")).alias(
            "perda_faturamento_pct"
        ),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze snis_indicador_agua: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
