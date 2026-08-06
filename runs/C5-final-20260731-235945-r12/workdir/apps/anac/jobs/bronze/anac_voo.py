from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("anac", "voo", "raw")
DESTINO = table_location("anac", "voo", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("id_voo", StringType()),
        StructField("empresa_icao", StringType()),
        StructField("origem_oaci", StringType()),
        StructField("destino_oaci", StringType()),
        StructField("data_partida", StringType()),
        StructField("passageiros", StringType()),
        StructField("assentos", StringType()),
    ]
)


def main():
    """Lê o raw dos voos, tipa as colunas de interesse e salva a bronze."""
    spark = SparkSession.builder.appName("anac-voo-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    typed = (
        raw.withColumn("data_partida", ct.parse_iso_timestamp(raw["data_partida"]))
        .withColumn("passageiros", raw["passageiros"].cast("int"))
        .withColumn("assentos", raw["assentos"].cast("int"))
    )

    bronze = typed.select(
        "id_voo",
        "empresa_icao",
        "origem_oaci",
        "destino_oaci",
        "data_partida",
        "passageiros",
        "assentos",
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze anac_voo: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
