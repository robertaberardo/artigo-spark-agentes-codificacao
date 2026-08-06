from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

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

# quantidades de matrículas por etapa; ausência permanece NULL (nunca zero)
QUANTIDADES = {
    "mat_basica": "MAT_BASICA",
    "mat_infantil": "MAT_INFANTIL",
    "mat_fundamental": "MAT_FUNDAMENTAL",
    "mat_medio": "MAT_MEDIO",
    "mat_profissional": "MAT_PROFISSIONAL",
    "mat_eja": "MAT_EJA",
    "mat_especial": "MAT_ESPECIAL",
}


def clean_matricula(df: DataFrame) -> DataFrame:
    """Tipa as quantidades como inteiro e padroniza os nomes para snake_case."""
    return df.select(
        ct.zero_pad_code(df["ID_UNIDADE"], 8).alias("id_unidade"),
        *[
            ct.blank_to_null(df[src]).cast("int").alias(dst)
            for dst, src in QUANTIDADES.items()
        ],
    )


def main():
    """Lê o raw das matrículas, tipa as quantidades e salva a bronze."""
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.transform(clean_matricula)

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
