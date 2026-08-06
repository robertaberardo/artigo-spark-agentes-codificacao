from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("tse", "municipio", "raw")
DESTINO = table_location("tse", "municipio", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("municipio_ibge", StringType()),
        StructField("nome", StringType()),
        StructField("uf", StringType()),
        StructField("geom_wkt", StringType()),
    ]
)


def main():
    """Lê o raw dos municípios, padroniza os campos e mantém o WKT na bronze."""
    spark = SparkSession.builder.appName("tse-municipio-bronze").getOrCreate()
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
        ct.blank_to_null(F.col("geom_wkt")).alias("geom_wkt"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze tse_municipio: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
