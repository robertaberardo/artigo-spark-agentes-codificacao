from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("ana", "reservatorio", "raw")
DESTINO = table_location("ana", "reservatorio", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("codigo", StringType()),
        StructField("nome", StringType()),
        StructField("uf", StringType()),
        StructField("capacidade_hm3", StringType()),
        StructField("latitude", StringType()),
        StructField("longitude", StringType()),
    ]
)


def main():
    """Lê o raw dos reservatórios, tipa capacidade e coordenadas na bronze."""
    spark = SparkSession.builder.appName("ana-reservatorio-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.upper(F.trim(raw["codigo"])).alias("codigo"),
        ct.normalize_text(raw["nome"]).alias("nome"),
        ct.standardize_uf(raw["uf"]).alias("uf"),
        ct.br_decimal_to_double(raw["capacidade_hm3"]).alias("capacidade_hm3"),
        ct.to_coordinate(raw["latitude"]).alias("latitude"),
        ct.to_coordinate(raw["longitude"]).alias("longitude"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze ana_reservatorio: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
