from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("tse", "partido", "raw")
DESTINO = table_location("tse", "partido", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("sigla", StringType()),
        StructField("nome", StringType()),
        StructField("numero", StringType()),
    ]
)


def main():
    """Lê o raw dos partidos, tipa o número de legenda e salva a bronze."""
    spark = SparkSession.builder.appName("tse-partido-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.upper(F.trim(raw["sigla"])).alias("sigla"),
        ct.normalize_text(raw["nome"]).alias("nome"),
        raw["numero"].cast("int").alias("numero"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze tse_partido: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
