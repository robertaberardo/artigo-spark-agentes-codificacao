from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

# CSV (header, separador ";") com todas as colunas em texto; como no raw de
# escola, há uma coluna final "ano" à direita que o schema explícito ignora.
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


def main():
    """Lê o raw de matrículas por unidade, padroniza nomes e tipa as contagens."""
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.trim(F.col("ID_UNIDADE")).alias("id_unidade"),
        F.col("MAT_BASICA").cast("int").alias("enrollment_basic"),
        F.col("MAT_INFANTIL").cast("int").alias("enrollment_infant"),
        F.col("MAT_FUNDAMENTAL").cast("int").alias("enrollment_elementary"),
        F.col("MAT_MEDIO").cast("int").alias("enrollment_highschool"),
        F.col("MAT_PROFISSIONAL").cast("int").alias("enrollment_professional"),
        F.col("MAT_EJA").cast("int").alias("enrollment_youth_adult"),
        F.col("MAT_ESPECIAL").cast("int").alias("enrollment_special"),
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
