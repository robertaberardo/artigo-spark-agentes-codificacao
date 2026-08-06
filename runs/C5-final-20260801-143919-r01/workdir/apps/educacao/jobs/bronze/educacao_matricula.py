from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

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

# As contagens de matrícula são inteiros; a caixa vira snake_case já no bronze.
MAT_COLUMNS = {
    "mat_basica": "MAT_BASICA",
    "mat_infantil": "MAT_INFANTIL",
    "mat_fundamental": "MAT_FUNDAMENTAL",
    "mat_medio": "MAT_MEDIO",
    "mat_profissional": "MAT_PROFISSIONAL",
    "mat_eja": "MAT_EJA",
    "mat_especial": "MAT_ESPECIAL",
}


def main():
    """Lê o raw das matrículas, converte as contagens em inteiros e grava a bronze."""
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(F.col("ID_UNIDADE"), 8).alias("id_unidade"),
        *[
            ct.blank_to_null(F.col(src)).cast(T.IntegerType()).alias(dst)
            for dst, src in MAT_COLUMNS.items()
        ],
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
