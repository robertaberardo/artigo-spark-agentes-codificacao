from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

# Schema do dado cru: nomes na caixa original da fonte.
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

# Contagens de matrícula (mesmo cast inteiro para todas).
ENROLLMENT_COLUMNS = {
    "MAT_BASICA": "mat_basica",
    "MAT_INFANTIL": "mat_infantil",
    "MAT_FUNDAMENTAL": "mat_fundamental",
    "MAT_MEDIO": "mat_medio",
    "MAT_PROFISSIONAL": "mat_profissional",
    "MAT_EJA": "mat_eja",
    "MAT_ESPECIAL": "mat_especial",
}


def main():
    """Lê o raw das matrículas, tipa as contagens como inteiro e salva a bronze."""
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
            F.col(src).cast("int").alias(dst)
            for src, dst in ENROLLMENT_COLUMNS.items()
        ],
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
