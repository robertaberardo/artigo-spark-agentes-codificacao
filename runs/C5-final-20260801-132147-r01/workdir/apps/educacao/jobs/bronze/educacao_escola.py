from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# Schema explícito do CSV bruto (nunca inferSchema); a ordem casa com o cabeçalho.
RAW_SCHEMA = T.StructType(
    [
        T.StructField("ID_UNIDADE", T.StringType()),
        T.StructField("NOME_UNIDADE", T.StringType()),
        T.StructField("UF", T.StringType()),
        T.StructField("COD_UF", T.StringType()),
        T.StructField("NOME_MUNICIPIO", T.StringType()),
        T.StructField("COD_MUNICIPIO", T.StringType()),
        T.StructField("SETOR", T.StringType()),
        T.StructField("AREA", T.StringType()),
        T.StructField("SITUACAO", T.StringType()),
        T.StructField("LATITUDE", T.StringType()),
        T.StructField("LONGITUDE", T.StringType()),
        T.StructField("QT_SALAS", T.StringType()),
        T.StructField("TEM_AGUA", T.StringType()),
        T.StructField("TEM_ENERGIA", T.StringType()),
        T.StructField("TEM_ESGOTO", T.StringType()),
        T.StructField("TEM_BANHEIRO", T.StringType()),
        T.StructField("TEM_BIBLIOTECA", T.StringType()),
        T.StructField("TEM_LAB_INFO", T.StringType()),
        T.StructField("TEM_QUADRA", T.StringType()),
        T.StructField("TEM_INTERNET", T.StringType()),
    ]
)

# Indicadores 0/1 de infraestrutura: mesmo tratamento para todos.
INFRA_FLAGS = [
    "tem_agua",
    "tem_energia",
    "tem_esgoto",
    "tem_banheiro",
    "tem_biblioteca",
    "tem_lab_info",
    "tem_quadra",
    "tem_internet",
]


def main():
    """Lê o raw das escolas, padroniza nomes/tipos e materializa a bronze."""
    spark = SparkSession.builder.appName("educacao-escola-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(raw["ID_UNIDADE"], 8).alias("id_unidade"),
        ct.normalize_text(raw["NOME_UNIDADE"]).alias("nome_unidade"),
        ct.standardize_uf(raw["UF"]).alias("uf"),
        ct.digits_only(raw["COD_UF"]).alias("cod_uf"),
        ct.normalize_text(raw["NOME_MUNICIPIO"]).alias("nome_municipio"),
        ct.digits_only(raw["COD_MUNICIPIO"]).alias("cod_municipio"),
        ct.blank_to_null(raw["SETOR"]).alias("setor"),
        ct.blank_to_null(raw["AREA"]).alias("area"),
        ct.blank_to_null(raw["SITUACAO"]).alias("situacao"),
        ct.to_coordinate(raw["LATITUDE"]).alias("latitude"),
        ct.to_coordinate(raw["LONGITUDE"]).alias("longitude"),
        ct.blank_to_null(raw["QT_SALAS"]).cast("int").alias("qt_salas"),
        *[
            ct.blank_to_null(raw[flag.upper()]).cast("int").alias(flag)
            for flag in INFRA_FLAGS
        ],
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
