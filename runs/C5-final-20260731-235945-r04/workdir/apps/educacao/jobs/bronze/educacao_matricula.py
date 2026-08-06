from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

# O CSV do censo aterrissa com contagens em texto e separador ";".
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

VALID_KEY = r"^[0-9]{8}$"


def main():
    """Lê o raw de matrículas, tipa as contagens e salva a bronze."""
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    id_unidade = ct.zero_pad_code(raw["ID_UNIDADE"], 8)
    bronze = raw.select(
        id_unidade.alias("id_unidade"),
        raw["MAT_BASICA"].cast("int").alias("mat_basica"),
        raw["MAT_INFANTIL"].cast("int").alias("mat_infantil"),
        raw["MAT_FUNDAMENTAL"].cast("int").alias("mat_fundamental"),
        raw["MAT_MEDIO"].cast("int").alias("mat_medio"),
        raw["MAT_PROFISSIONAL"].cast("int").alias("mat_profissional"),
        raw["MAT_EJA"].cast("int").alias("mat_eja"),
        raw["MAT_ESPECIAL"].cast("int").alias("mat_especial"),
    ).filter(id_unidade.rlike(VALID_KEY))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
