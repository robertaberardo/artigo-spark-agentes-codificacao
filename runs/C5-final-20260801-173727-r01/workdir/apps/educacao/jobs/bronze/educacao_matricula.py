from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

ID_WIDTH = 8

RAW_SCHEMA = T.StructType(
    [
        T.StructField("ID_UNIDADE", T.StringType()),
        T.StructField("MAT_BASICA", T.StringType()),
        T.StructField("MAT_INFANTIL", T.StringType()),
        T.StructField("MAT_FUNDAMENTAL", T.StringType()),
        T.StructField("MAT_MEDIO", T.StringType()),
        T.StructField("MAT_PROFISSIONAL", T.StringType()),
        T.StructField("MAT_EJA", T.StringType()),
        T.StructField("MAT_ESPECIAL", T.StringType()),
    ]
)

# contagens de matrícula por etapa de ensino
COUNT_COLUMNS = [
    "MAT_BASICA",
    "MAT_INFANTIL",
    "MAT_FUNDAMENTAL",
    "MAT_MEDIO",
    "MAT_PROFISSIONAL",
    "MAT_EJA",
    "MAT_ESPECIAL",
]


def to_count(name: str) -> F.Column:
    """Converte uma contagem de matrículas (texto) em ``int``, ausência como ``NULL``."""
    return F.trim(F.col(name)).cast("int").alias(name.lower())


def main():
    """Lê o raw das matrículas, padroniza nomes e tipa as contagens para a bronze."""
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    id_unidade = ct.zero_pad_code(F.col("ID_UNIDADE"), ID_WIDTH)

    bronze = raw.select(
        id_unidade.alias("id_unidade"),
        *[to_count(c) for c in COUNT_COLUMNS],
    ).filter(id_unidade.isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
