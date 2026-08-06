from pyspark.sql import SparkSession
from pyspark.sql import types as T, functions as F

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

ID_WIDTH = 8

# Schema aplicado por posição às colunas do CSV (header é descartado); os nomes
# já entram em snake_case, a padronização de caixa do bronze.
RAW_SCHEMA = T.StructType(
    [
        T.StructField("id_unidade", T.StringType()),
        T.StructField("nome_unidade", T.StringType()),
        T.StructField("uf", T.StringType()),
        T.StructField("cod_uf", T.StringType()),
        T.StructField("nome_municipio", T.StringType()),
        T.StructField("cod_municipio", T.StringType()),
        T.StructField("setor", T.StringType()),
        T.StructField("area", T.StringType()),
        T.StructField("situacao", T.StringType()),
        T.StructField("latitude", T.StringType()),
        T.StructField("longitude", T.StringType()),
        T.StructField("qt_salas", T.StringType()),
        T.StructField("tem_agua", T.StringType()),
        T.StructField("tem_energia", T.StringType()),
        T.StructField("tem_esgoto", T.StringType()),
        T.StructField("tem_banheiro", T.StringType()),
        T.StructField("tem_biblioteca", T.StringType()),
        T.StructField("tem_lab_info", T.StringType()),
        T.StructField("tem_quadra", T.StringType()),
        T.StructField("tem_internet", T.StringType()),
    ]
)

INDICATORS = [
    "tem_agua", "tem_energia", "tem_esgoto", "tem_banheiro",
    "tem_biblioteca", "tem_lab_info", "tem_quadra", "tem_internet",
]


def main():
    """Lê o raw das escolas, padroniza nomes/tipos e grava a bronze em parquet."""
    spark = SparkSession.builder.appName("educacao-escola-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(F.col("id_unidade"), ID_WIDTH).alias("id_unidade"),
        ct.normalize_text(F.col("nome_unidade")).alias("nome_unidade"),
        ct.standardize_uf(F.col("uf")).alias("uf"),
        ct.digits_only(F.col("cod_uf")).alias("cod_uf"),
        ct.normalize_text(F.col("nome_municipio")).alias("nome_municipio"),
        ct.digits_only(F.col("cod_municipio")).alias("cod_municipio"),
        F.col("setor").cast("int").alias("setor"),
        F.col("area").cast("int").alias("area"),
        F.col("situacao").cast("int").alias("situacao"),
        ct.to_coordinate(F.col("latitude")).alias("latitude"),
        ct.to_coordinate(F.col("longitude")).alias("longitude"),
        F.col("qt_salas").cast("int").alias("qt_salas"),
        *[F.col(c).cast("int").alias(c) for c in INDICATORS],
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
