from pyspark.sql import SparkSession
from pyspark.sql import types as T, functions as F

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

ID_WIDTH = 8

RAW_SCHEMA = T.StructType(
    [
        T.StructField("id_unidade", T.StringType()),
        T.StructField("mat_basica", T.StringType()),
        T.StructField("mat_infantil", T.StringType()),
        T.StructField("mat_fundamental", T.StringType()),
        T.StructField("mat_medio", T.StringType()),
        T.StructField("mat_profissional", T.StringType()),
        T.StructField("mat_eja", T.StringType()),
        T.StructField("mat_especial", T.StringType()),
    ]
)

# Colunas de contagem de matrícula: ausência fica NULL (não é zero).
COUNT_COLUMNS = [
    "mat_basica", "mat_infantil", "mat_fundamental", "mat_medio",
    "mat_profissional", "mat_eja", "mat_especial",
]


def main():
    """Lê o raw das matrículas, tipa as contagens e grava a bronze em parquet."""
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(F.col("id_unidade"), ID_WIDTH).alias("id_unidade"),
        *[F.col(c).cast("int").alias(c) for c in COUNT_COLUMNS],
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
