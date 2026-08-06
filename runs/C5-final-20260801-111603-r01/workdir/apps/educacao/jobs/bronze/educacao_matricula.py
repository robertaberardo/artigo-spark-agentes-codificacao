from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from apps.educacao.utils import column_transforms as edu_ct
from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

# O raw traz uma coluna extra `ano` ao final; o schema por posição a descarta.
RAW_SCHEMA = StructType(
    [
        StructField("ID_UNIDADE", StringType()),
        StructField("MAT_BASICA", StringType()),
        StructField("MAT_INFANTIL", StringType()),
        StructField("MAT_FUNDAMENTAL", StringType()),
        StructField("MAT_MEDIO", StringType()),
        StructField("MAT_PROFISSIONAL", StringType()),
        StructField("MAT_EJA", StringType()),
        StructField("MAT_ESPECIAL", StringType()),
    ]
)

MATRICULAS = [
    "MAT_BASICA",
    "MAT_INFANTIL",
    "MAT_FUNDAMENTAL",
    "MAT_MEDIO",
    "MAT_PROFISSIONAL",
    "MAT_EJA",
    "MAT_ESPECIAL",
]


def main():
    """Lê o raw das matrículas, tipa as colunas e salva a bronze."""
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(raw["ID_UNIDADE"], 8).alias("id_unidade"),
        *[raw[c].cast("int").alias(c.lower()) for c in MATRICULAS],
    ).filter(edu_ct.is_valid_unidade_code(raw["ID_UNIDADE"]))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
