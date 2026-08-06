from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

# Colunas de contagem de matrícula: todas viram int no bronze.
COUNT_COLS = [
    "MAT_BASICA",
    "MAT_INFANTIL",
    "MAT_FUNDAMENTAL",
    "MAT_MEDIO",
    "MAT_PROFISSIONAL",
    "MAT_EJA",
    "MAT_ESPECIAL",
]

RAW_SCHEMA = StructType(
    [StructField("ID_UNIDADE", StringType())]
    + [StructField(c, StringType()) for c in COUNT_COLS]
)


def main():
    """Lê o raw das matrículas e tipa as contagens como inteiro."""
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    # Ausência de contagem fica como null (nunca 0): o cast de string vazia para
    # int já devolve null, preservando a semântica de ausência.
    bronze = raw.select(
        ct.zero_pad_code(raw["ID_UNIDADE"], 8).alias("id_unidade"),
        *[raw[c].cast("int").alias(c.lower()) for c in COUNT_COLS],
    ).filter(ct.zero_pad_code(raw["ID_UNIDADE"], 8).isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
