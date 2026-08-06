from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# O CSV do Censo Escolar chega em maiúsculas e separado por ';'. O schema é
# aplicado por posição (header descartado), já renomeando para snake_case.
RAW_SCHEMA = StructType(
    [
        StructField("id_unidade", StringType()),
        StructField("nome_unidade", StringType()),
        StructField("uf", StringType()),
        StructField("cod_uf", StringType()),
        StructField("nome_municipio", StringType()),
        StructField("cod_municipio", StringType()),
        StructField("setor", StringType()),
        StructField("area", StringType()),
        StructField("situacao", StringType()),
        StructField("latitude", StringType()),
        StructField("longitude", StringType()),
        StructField("qt_salas", StringType()),
        StructField("tem_agua", StringType()),
        StructField("tem_energia", StringType()),
        StructField("tem_esgoto", StringType()),
        StructField("tem_banheiro", StringType()),
        StructField("tem_biblioteca", StringType()),
        StructField("tem_lab_info", StringType()),
        StructField("tem_quadra", StringType()),
        StructField("tem_internet", StringType()),
    ]
)


def main():
    """Lê o raw das escolas, tipa as colunas e grava a bronze."""
    spark = SparkSession.builder.appName("educacao-escola-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(raw["id_unidade"], 8).alias("id_unidade"),
        ct.normalize_text(raw["nome_unidade"]).alias("nome_unidade"),
        ct.standardize_uf(raw["uf"]).alias("uf"),
        ct.digits_only(raw["cod_municipio"]).alias("cod_municipio"),
        raw["setor"].cast("int").alias("setor"),
        raw["area"].cast("int").alias("area"),
        raw["situacao"].cast("int").alias("situacao"),
        ct.to_coordinate(raw["latitude"]).alias("latitude"),
        ct.to_coordinate(raw["longitude"]).alias("longitude"),
        raw["qt_salas"].cast("int").alias("qt_salas"),
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
