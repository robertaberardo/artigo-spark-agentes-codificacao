from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

ID_WIDTH = 8

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

# indicadores de infraestrutura (0/1) que só precisam de dígitos + cast int
FLAG_COLUMNS = [
    "TEM_AGUA",
    "TEM_ENERGIA",
    "TEM_ESGOTO",
    "TEM_BANHEIRO",
    "TEM_BIBLIOTECA",
    "TEM_LAB_INFO",
    "TEM_QUADRA",
    "TEM_INTERNET",
]


def to_int(name: str) -> F.Column:
    """Converte um código numérico curto (texto, sem zeros à esquerda) em ``int``.

    Faz ``trim`` antes do cast para tolerar espaços; texto vazio ou inválido vira
    ``NULL`` (ausência nunca é zero).
    """
    return F.trim(F.col(name)).cast("int").alias(name.lower())


def main():
    """Lê o raw das escolas, padroniza nomes e tipa as colunas para a bronze."""
    spark = SparkSession.builder.appName("educacao-escola-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    id_unidade = ct.zero_pad_code(F.col("ID_UNIDADE"), ID_WIDTH)

    bronze = raw.select(
        id_unidade.alias("id_unidade"),
        ct.normalize_text(F.col("NOME_UNIDADE")).alias("nome_unidade"),
        ct.standardize_uf(F.col("UF")).alias("uf"),
        ct.digits_only(F.col("COD_UF")).alias("cod_uf"),
        ct.normalize_text(F.col("NOME_MUNICIPIO")).alias("nome_municipio"),
        ct.digits_only(F.col("COD_MUNICIPIO")).alias("cod_municipio"),
        to_int("SETOR"),
        to_int("AREA"),
        to_int("SITUACAO"),
        ct.to_coordinate(F.col("LATITUDE")).alias("latitude"),
        ct.to_coordinate(F.col("LONGITUDE")).alias("longitude"),
        to_int("QT_SALAS"),
        *[to_int(c) for c in FLAG_COLUMNS],
    ).filter(id_unidade.isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
