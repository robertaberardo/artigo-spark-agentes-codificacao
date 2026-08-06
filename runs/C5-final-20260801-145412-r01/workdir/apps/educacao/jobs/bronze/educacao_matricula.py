from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

RAW_SCHEMA = T.StructType(
    [
        T.StructField(name, T.StringType())
        for name in [
            "ID_UNIDADE", "MAT_BASICA", "MAT_INFANTIL", "MAT_FUNDAMENTAL",
            "MAT_MEDIO", "MAT_PROFISSIONAL", "MAT_EJA", "MAT_ESPECIAL",
        ]
    ]
)

# Colunas de contagem: mesmo alias em snake_case, todas casteadas para int.
COUNT_COLUMNS = {
    "MAT_BASICA": "mat_basica",
    "MAT_INFANTIL": "mat_infantil",
    "MAT_FUNDAMENTAL": "mat_fundamental",
    "MAT_MEDIO": "mat_medio",
    "MAT_PROFISSIONAL": "mat_profissional",
    "MAT_EJA": "mat_eja",
    "MAT_ESPECIAL": "mat_especial",
}


def main():
    """Lê o raw das matrículas, normaliza a chave e as contagens, grava a bronze."""
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
            ct.blank_to_null(F.col(src)).cast("int").alias(dst)
            for src, dst in COUNT_COLUMNS.items()
        ],
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
