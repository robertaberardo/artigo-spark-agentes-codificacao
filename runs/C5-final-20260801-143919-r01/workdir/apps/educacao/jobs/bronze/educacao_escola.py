from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

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


def main():
    """Lê o raw das escolas do Censo, tipa os campos e grava a bronze.

    Códigos administrativos (setor/área/situação) permanecem como texto — são
    categorias, não quantidades. Lat/long viram ``double`` para o geoespacial;
    a chave é normalizada para 8 dígitos com zero à esquerda, casando com o
    formato das matrículas.
    """
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
        ct.blank_to_null(F.col("COD_UF")).alias("cod_uf"),
        ct.normalize_text(F.col("NOME_MUNICIPIO")).alias("nome_municipio"),
        ct.blank_to_null(F.col("COD_MUNICIPIO")).alias("cod_municipio"),
        ct.blank_to_null(F.col("SETOR")).alias("setor"),
        ct.blank_to_null(F.col("AREA")).alias("area"),
        ct.blank_to_null(F.col("SITUACAO")).alias("situacao"),
        ct.to_coordinate(F.col("LATITUDE")).alias("latitude"),
        ct.to_coordinate(F.col("LONGITUDE")).alias("longitude"),
        ct.blank_to_null(F.col("QT_SALAS")).cast(T.IntegerType()).alias("qt_salas"),
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
