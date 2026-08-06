from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

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


def main():
    """Lê o raw das escolas, padroniza nomes/tipos e salva a bronze.

    Nomes de coluna viram snake_case; lat/long viram ``double`` (nunca string) e
    os indicadores 0/1 viram ``int``. A ausência é preservada como ``NULL``.
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
        ct.blank_to_null(F.col("QT_SALAS")).cast("int").alias("qt_salas"),
        ct.blank_to_null(F.col("TEM_AGUA")).cast("int").alias("tem_agua"),
        ct.blank_to_null(F.col("TEM_ENERGIA")).cast("int").alias("tem_energia"),
        ct.blank_to_null(F.col("TEM_ESGOTO")).cast("int").alias("tem_esgoto"),
        ct.blank_to_null(F.col("TEM_BANHEIRO")).cast("int").alias("tem_banheiro"),
        ct.blank_to_null(F.col("TEM_BIBLIOTECA")).cast("int").alias("tem_biblioteca"),
        ct.blank_to_null(F.col("TEM_LAB_INFO")).cast("int").alias("tem_lab_info"),
        ct.blank_to_null(F.col("TEM_QUADRA")).cast("int").alias("tem_quadra"),
        ct.blank_to_null(F.col("TEM_INTERNET")).cast("int").alias("tem_internet"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
