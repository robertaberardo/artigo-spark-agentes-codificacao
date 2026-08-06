from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# Schema do dado cru: nomes na caixa original da fonte; a padronização para
# snake_case acontece nos alias do select do bronze.
RAW_SCHEMA = StructType(
    [
        StructField("ID_UNIDADE", StringType()),
        StructField("NOME_UNIDADE", StringType()),
        StructField("UF", StringType()),
        StructField("COD_UF", StringType()),
        StructField("NOME_MUNICIPIO", StringType()),
        StructField("COD_MUNICIPIO", StringType()),
        StructField("SETOR", StringType()),
        StructField("AREA", StringType()),
        StructField("SITUACAO", StringType()),
        StructField("LATITUDE", StringType()),
        StructField("LONGITUDE", StringType()),
        StructField("QT_SALAS", StringType()),
        StructField("TEM_AGUA", StringType()),
        StructField("TEM_ENERGIA", StringType()),
        StructField("TEM_ESGOTO", StringType()),
        StructField("TEM_BANHEIRO", StringType()),
        StructField("TEM_BIBLIOTECA", StringType()),
        StructField("TEM_LAB_INFO", StringType()),
        StructField("TEM_QUADRA", StringType()),
        StructField("TEM_INTERNET", StringType()),
    ]
)

# Indicadores de infraestrutura (0/1) — mesmo tratamento para todos.
INDICATOR_COLUMNS = {
    "TEM_AGUA": "tem_agua",
    "TEM_ENERGIA": "tem_energia",
    "TEM_ESGOTO": "tem_esgoto",
    "TEM_BANHEIRO": "tem_banheiro",
    "TEM_BIBLIOTECA": "tem_biblioteca",
    "TEM_LAB_INFO": "tem_lab_info",
    "TEM_QUADRA": "tem_quadra",
    "TEM_INTERNET": "tem_internet",
}


def main():
    """Lê o raw das escolas do Censo Escolar, limpa/tipa colunas e salva a bronze."""
    spark = SparkSession.builder.appName("educacao-escola-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        # chave normalizada com zero à esquerda, para casar com a matrícula
        ct.zero_pad_code(F.col("ID_UNIDADE"), 8).alias("id_unidade"),
        ct.normalize_text(F.col("NOME_UNIDADE")).alias("nome_unidade"),
        ct.standardize_uf(F.col("UF")).alias("uf"),
        ct.digits_only(F.col("COD_UF")).alias("cod_uf"),
        ct.normalize_text(F.col("NOME_MUNICIPIO")).alias("nome_municipio"),
        ct.digits_only(F.col("COD_MUNICIPIO")).alias("cod_municipio"),
        ct.blank_to_null(F.col("SETOR")).alias("setor"),
        ct.blank_to_null(F.col("AREA")).alias("area"),
        ct.blank_to_null(F.col("SITUACAO")).alias("situacao"),
        ct.to_coordinate(F.col("LATITUDE")).alias("latitude"),
        ct.to_coordinate(F.col("LONGITUDE")).alias("longitude"),
        F.col("QT_SALAS").cast("int").alias("qt_salas"),
        *[
            ct.yes_no_to_int(F.col(src)).alias(dst)
            for src, dst in INDICATOR_COLUMNS.items()
        ],
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
