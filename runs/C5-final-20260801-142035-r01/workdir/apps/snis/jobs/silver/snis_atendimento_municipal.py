from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils import dataframe_transforms as dt
from utils.metadata import table_location

AGUA = table_location("snis", "indicador_agua", "raw")
ESGOTO = table_location("snis", "indicador_esgoto", "raw")
MUNICIPIO = table_location("snis", "municipio", "bronze")
DESTINO = table_location("snis", "atendimento_municipal", "silver")

AGUA_SCHEMA = StructType(
    [
        StructField("municipio_ibge", StringType()),
        StructField("ano", StringType()),
        StructField("indice_atendimento_agua_pct", StringType()),
        StructField("consumo_medio_l_hab_dia", StringType()),
        StructField("perda_faturamento_pct", StringType()),
    ]
)

ESGOTO_SCHEMA = StructType(
    [
        StructField("municipio_ibge", StringType()),
        StructField("ano", StringType()),
        StructField("indice_coleta_esgoto_pct", StringType()),
        StructField("indice_tratamento_esgoto_pct", StringType()),
    ]
)

COVERAGE_BOUNDS = [0.4, 0.8]
COVERAGE_LABELS = ["baixo", "medio", "alto"]


def read_indicador_agua(spark):
    """Lê o cru de água, converte os percentuais e tipa o consumo."""
    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(AGUA_SCHEMA)
        .csv(AGUA)
    )
    return raw.select(
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


def read_indicador_esgoto(spark):
    """Lê o cru de esgoto e converte os percentuais em fração."""
    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(ESGOTO_SCHEMA)
        .csv(ESGOTO)
    )
    return raw.select(
        ct.zero_pad_code(F.col("municipio_ibge"), 7).alias("municipio_ibge"),
        F.col("ano").cast("int").alias("ano"),
        ct.percent_to_fraction(F.col("indice_coleta_esgoto_pct")).alias(
            "indice_coleta_esgoto_pct"
        ),
        ct.percent_to_fraction(F.col("indice_tratamento_esgoto_pct")).alias(
            "indice_tratamento_esgoto_pct"
        ),
    )


def classify_coverage(df):
    """Limita os índices ao intervalo [0,1] e classifica o atendimento de água."""
    return (
        df.withColumn(
            "indice_atendimento_agua_pct",
            ct.clamp(F.col("indice_atendimento_agua_pct"), 0.0, 1.0),
        )
        .withColumn(
            "indice_coleta_esgoto_pct",
            ct.clamp(F.col("indice_coleta_esgoto_pct"), 0.0, 1.0),
        )
        .withColumn(
            "indice_tratamento_esgoto_pct",
            ct.clamp(F.col("indice_tratamento_esgoto_pct"), 0.0, 1.0),
        )
        .withColumn(
            "classe_atendimento",
            ct.bucketize(
                F.col("indice_atendimento_agua_pct"), COVERAGE_BOUNDS, COVERAGE_LABELS
            ),
        )
    )


def main():
    """Integra os indicadores municipais de água e esgoto com dados do município."""
    spark = SparkSession.builder.appName(
        "snis-atendimento-municipal-silver"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    agua = read_indicador_agua(spark)
    esgoto = read_indicador_esgoto(spark)
    municipio = spark.read.parquet(MUNICIPIO)

    result = (
        agua.join(esgoto, on=["municipio_ibge", "ano"], how="outer")
        .transform(
            lambda d: dt.join_lookup(
                d, municipio, "municipio_ibge", ["uf", "populacao"]
            )
        )
        .transform(classify_coverage)
        .select(
            "municipio_ibge",
            "ano",
            "uf",
            "populacao",
            "indice_atendimento_agua_pct",
            "indice_coleta_esgoto_pct",
            "indice_tratamento_esgoto_pct",
            "consumo_medio_l_hab_dia",
            "perda_faturamento_pct",
            "classe_atendimento",
        )
    )

    result.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> silver snis_atendimento_municipal: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
