from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# Indicadores de infraestrutura (0/1). Ficam num grupo à parte porque recebem o
# mesmo tratamento (yes_no_to_int) e viram int.
INDICATOR_COLS = [
    "TEM_AGUA",
    "TEM_ENERGIA",
    "TEM_ESGOTO",
    "TEM_BANHEIRO",
    "TEM_BIBLIOTECA",
    "TEM_LAB_INFO",
    "TEM_QUADRA",
    "TEM_INTERNET",
]

# Raw é todo string; o schema explícito evita inferSchema e fixa o contrato.
RAW_COLS = [
    "ID_UNIDADE",
    "NOME_UNIDADE",
    "UF",
    "COD_UF",
    "NOME_MUNICIPIO",
    "COD_MUNICIPIO",
    "SETOR",
    "AREA",
    "SITUACAO",
    "LATITUDE",
    "LONGITUDE",
    "QT_SALAS",
    *INDICATOR_COLS,
]
RAW_SCHEMA = StructType([StructField(c, StringType()) for c in RAW_COLS])


def main():
    """Lê o raw das escolas, aplica os tipos reais e grava a bronze."""
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
        raw["QT_SALAS"].cast("int").alias("qt_salas"),
        *[ct.yes_no_to_int(raw[c]).alias(c.lower()) for c in INDICATOR_COLS],
    ).filter(ct.zero_pad_code(raw["ID_UNIDADE"], 8).isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
