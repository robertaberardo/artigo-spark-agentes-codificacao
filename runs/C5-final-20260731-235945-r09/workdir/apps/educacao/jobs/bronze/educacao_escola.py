from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# O raw aterrissa como CSV (header, separador ";") com todas as colunas em texto.
# O arquivo traz ainda uma coluna final "ano" que não faz parte da chave do
# domínio; como o schema explícito cobre só as colunas declaradas, o Spark ignora
# a coluna extra à direita.
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
    """Lê o raw das escolas (Censo Escolar), padroniza nomes e tipa as colunas."""
    spark = SparkSession.builder.appName("educacao-escola-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.trim(F.col("ID_UNIDADE")).alias("id_unidade"),
        ct.normalize_text(F.col("NOME_UNIDADE")).alias("school_name"),
        ct.standardize_uf(F.col("UF")).alias("uf"),
        F.trim(F.col("COD_UF")).alias("uf_code"),
        ct.normalize_text(F.col("NOME_MUNICIPIO")).alias("municipality_name"),
        F.trim(F.col("COD_MUNICIPIO")).alias("municipality_code"),
        F.col("SETOR").cast("int").alias("admin_sector"),
        F.col("AREA").cast("int").alias("area"),
        F.col("SITUACAO").cast("int").alias("operating_status"),
        ct.to_coordinate(F.col("LATITUDE")).alias("latitude"),
        ct.to_coordinate(F.col("LONGITUDE")).alias("longitude"),
        F.col("QT_SALAS").cast("int").alias("classroom_count"),
        F.col("TEM_AGUA").cast("int").alias("has_water"),
        F.col("TEM_ENERGIA").cast("int").alias("has_electricity"),
        F.col("TEM_ESGOTO").cast("int").alias("has_sewage"),
        F.col("TEM_BANHEIRO").cast("int").alias("has_bathroom"),
        F.col("TEM_BIBLIOTECA").cast("int").alias("has_library"),
        F.col("TEM_LAB_INFO").cast("int").alias("has_computer_lab"),
        F.col("TEM_QUADRA").cast("int").alias("has_sports_court"),
        F.col("TEM_INTERNET").cast("int").alias("has_internet"),
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
