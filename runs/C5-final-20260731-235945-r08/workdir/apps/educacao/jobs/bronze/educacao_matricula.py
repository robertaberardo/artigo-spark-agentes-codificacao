from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

# Contagens de matrícula aterrissam como texto; entram como string e viram int
# no select. Ausência permanece NULL (nunca zero): matrícula ausente não é zero.
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
    """Lê o raw das matrículas, padroniza nomes/tipos e grava a bronze."""
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
        ct.blank_to_null(raw["MAT_BASICA"]).cast("int").alias("mat_basica"),
        ct.blank_to_null(raw["MAT_INFANTIL"]).cast("int").alias("mat_infantil"),
        ct.blank_to_null(raw["MAT_FUNDAMENTAL"]).cast("int").alias("mat_fundamental"),
        ct.blank_to_null(raw["MAT_MEDIO"]).cast("int").alias("mat_medio"),
        ct.blank_to_null(raw["MAT_PROFISSIONAL"]).cast("int").alias("mat_profissional"),
        ct.blank_to_null(raw["MAT_EJA"]).cast("int").alias("mat_eja"),
        ct.blank_to_null(raw["MAT_ESPECIAL"]).cast("int").alias("mat_especial"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
