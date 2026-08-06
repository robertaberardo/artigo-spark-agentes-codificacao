from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("dnit", "acidente", "raw")
DESTINO = table_location("dnit", "acidente", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("br", StringType()),
        StructField("km", StringType()),
        StructField("uf", StringType()),
        StructField("data", StringType()),
        StructField("mortos", StringType()),
        StructField("feridos", StringType()),
        StructField("latitude", StringType()),
        StructField("longitude", StringType()),
    ]
)


def main():
    """Lê o raw dos acidentes, tipa data, vítimas e coordenadas e salva a bronze."""
    spark = SparkSession.builder.appName("dnit-acidente-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        raw["id"].cast("string").alias("id"),
        raw["br"].cast("string").alias("br"),
        raw["km"].cast("double").alias("km"),
        ct.standardize_uf(raw["uf"]).alias("uf"),
        ct.parse_br_date(raw["data"]).alias("data"),
        raw["mortos"].cast("int").alias("mortos"),
        raw["feridos"].cast("int").alias("feridos"),
        ct.to_coordinate(raw["latitude"]).alias("latitude"),
        ct.to_coordinate(raw["longitude"]).alias("longitude"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze dnit_acidente: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
