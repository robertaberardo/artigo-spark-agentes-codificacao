from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from apps.educacao.utils import column_transforms as educacao_ct
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


def main():
    """Lê o raw das matrículas, tipa as contagens como ``int`` e salva a bronze.

    As linhas com ``ID_UNIDADE`` malformado são descartadas para casar com a chave
    das escolas.
    """
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
        ct.blank_to_null(raw["MAT_BASICA"]).cast("int").alias("enrollment_basic"),
        ct.blank_to_null(raw["MAT_INFANTIL"]).cast("int").alias("enrollment_infant"),
        ct.blank_to_null(raw["MAT_FUNDAMENTAL"]).cast("int").alias("enrollment_elementary"),
        ct.blank_to_null(raw["MAT_MEDIO"]).cast("int").alias("enrollment_highschool"),
        ct.blank_to_null(raw["MAT_PROFISSIONAL"]).cast("int").alias("enrollment_professional"),
        ct.blank_to_null(raw["MAT_EJA"]).cast("int").alias("enrollment_youth_adult"),
        ct.blank_to_null(raw["MAT_ESPECIAL"]).cast("int").alias("enrollment_special"),
    ).filter(educacao_ct.is_valid_id_unidade(raw["ID_UNIDADE"]))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
