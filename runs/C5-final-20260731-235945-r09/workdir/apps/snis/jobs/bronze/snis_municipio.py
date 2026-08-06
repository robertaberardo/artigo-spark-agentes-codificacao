from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.snis.utils import column_transforms as snis_ct
from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("snis", "municipio", "raw")
DESTINO = table_location("snis", "municipio", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("municipio_ibge", StringType()),
        StructField("nome", StringType()),
        StructField("uf", StringType()),
        StructField("populacao", StringType()),
    ]
)


def main():
    """Lê o raw dos municípios, tipa a população e salva a bronze."""
    spark = SparkSession.builder.appName("snis-municipio-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(F.col("municipio_ibge"), 7).alias("municipio_ibge"),
        ct.normalize_text(F.col("nome")).alias("nome"),
        ct.standardize_uf(F.col("uf")).alias("uf"),
        ct.digits_only(F.col("populacao")).cast("long").alias("populacao"),
    ).filter(snis_ct.is_valid_ibge_municipio(F.col("municipio_ibge")))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze snis_municipio: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
