from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.educacao.utils import column_transforms as educacao_ct
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
    """Lê o raw das escolas, tipa/limpa as colunas e salva a bronze.

    Coordenadas viram ``double``, os indicadores 0/1 viram ``int`` e as linhas com
    ``ID_UNIDADE`` malformado são descartadas.
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
        ct.zero_pad_code(raw["ID_UNIDADE"], 8).alias("id_unidade"),
        ct.normalize_text(raw["NOME_UNIDADE"]).alias("name"),
        ct.standardize_uf(raw["UF"]).alias("uf"),
        ct.blank_to_null(raw["COD_UF"]).alias("cod_uf"),
        ct.normalize_text(raw["NOME_MUNICIPIO"]).alias("name_municipio"),
        ct.blank_to_null(raw["COD_MUNICIPIO"]).alias("cod_municipio"),
        ct.blank_to_null(raw["SETOR"]).alias("setor"),
        ct.blank_to_null(raw["AREA"]).alias("area"),
        ct.blank_to_null(raw["SITUACAO"]).alias("situacao"),
        ct.to_coordinate(raw["LATITUDE"]).alias("latitude"),
        ct.to_coordinate(raw["LONGITUDE"]).alias("longitude"),
        ct.blank_to_null(raw["QT_SALAS"]).cast("int").alias("qt_salas"),
        ct.yes_no_to_int(raw["TEM_AGUA"]).alias("has_water"),
        ct.yes_no_to_int(raw["TEM_ENERGIA"]).alias("has_power"),
        ct.yes_no_to_int(raw["TEM_ESGOTO"]).alias("has_sewage"),
        ct.yes_no_to_int(raw["TEM_BANHEIRO"]).alias("has_bathroom"),
        ct.yes_no_to_int(raw["TEM_BIBLIOTECA"]).alias("has_library"),
        ct.yes_no_to_int(raw["TEM_LAB_INFO"]).alias("has_computer_lab"),
        ct.yes_no_to_int(raw["TEM_QUADRA"]).alias("has_sports_court"),
        ct.yes_no_to_int(raw["TEM_INTERNET"]).alias("has_internet"),
    ).filter(educacao_ct.is_valid_id_unidade(raw["ID_UNIDADE"]))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
