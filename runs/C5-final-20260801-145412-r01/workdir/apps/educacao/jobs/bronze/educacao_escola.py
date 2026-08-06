from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# Schema explícito do raw (todas as colunas como string, ordem do cabeçalho).
RAW_SCHEMA = T.StructType(
    [
        T.StructField(name, T.StringType())
        for name in [
            "ID_UNIDADE", "NOME_UNIDADE", "UF", "COD_UF", "NOME_MUNICIPIO",
            "COD_MUNICIPIO", "SETOR", "AREA", "SITUACAO", "LATITUDE", "LONGITUDE",
            "QT_SALAS", "TEM_AGUA", "TEM_ENERGIA", "TEM_ESGOTO", "TEM_BANHEIRO",
            "TEM_BIBLIOTECA", "TEM_LAB_INFO", "TEM_QUADRA", "TEM_INTERNET",
        ]
    ]
)


def main():
    """Lê o raw das escolas, padroniza nomes/tipos e grava a bronze."""
    spark = SparkSession.builder.appName("educacao-escola-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(F.col("ID_UNIDADE"), 8).alias("id_unidade"),
        ct.normalize_text(F.col("NOME_UNIDADE")).alias("nome_unidade"),
        ct.standardize_uf(F.col("UF")).alias("uf"),
        ct.normalize_text(F.col("NOME_MUNICIPIO")).alias("nome_municipio"),
        ct.blank_to_null(F.col("SETOR")).cast("int").alias("setor"),
        ct.blank_to_null(F.col("AREA")).cast("int").alias("area"),
        ct.blank_to_null(F.col("SITUACAO")).cast("int").alias("situacao"),
        ct.to_coordinate(F.col("LATITUDE")).alias("latitude"),
        ct.to_coordinate(F.col("LONGITUDE")).alias("longitude"),
        ct.blank_to_null(F.col("QT_SALAS")).cast("int").alias("qt_salas"),
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
