from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.datasus.utils import column_transforms as datasus_ct
from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("datasus", "estabelecimento", "raw")
DESTINO = table_location("datasus", "estabelecimento", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("co_cnes", StringType()),
        StructField("nome", StringType()),
        StructField("municipio_ibge", StringType()),
        StructField("uf", StringType()),
        StructField("tipo", StringType()),
        StructField("latitude", StringType()),
        StructField("longitude", StringType()),
        StructField("leitos_sus", StringType()),
    ]
)


def main():
    """Lê o raw dos estabelecimentos, tipa as colunas e salva a bronze."""
    spark = SparkSession.builder.appName("datasus-estabelecimento-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(raw["co_cnes"], 7).alias("co_cnes"),
        ct.normalize_text(raw["nome"]).alias("nome"),
        ct.digits_only(raw["municipio_ibge"]).alias("municipio_ibge"),
        ct.standardize_uf(raw["uf"]).alias("uf"),
        ct.blank_to_null(raw["tipo"]).alias("tipo"),
        ct.to_coordinate(raw["latitude"]).alias("latitude"),
        ct.to_coordinate(raw["longitude"]).alias("longitude"),
        ct.coalesce_zero(raw["leitos_sus"].cast("int")).alias("leitos_sus"),
    ).filter(datasus_ct.is_valid_cnes(raw["co_cnes"]))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze datasus_estabelecimento: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
