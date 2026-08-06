from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("tse", "votacao", "raw")
DESTINO = table_location("tse", "votacao", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("id_votacao", StringType()),
        StructField("id_candidato", StringType()),
        StructField("municipio_ibge", StringType()),
        StructField("uf", StringType()),
        StructField("zona", StringType()),
        StructField("votos", StringType()),
    ]
)


def main():
    """Lê o raw da votação, tipa as colunas de interesse e salva a bronze."""
    spark = SparkSession.builder.appName("tse-votacao-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.upper(F.trim(F.col("id_votacao"))).alias("id_votacao"),
        F.upper(F.trim(F.col("id_candidato"))).alias("id_candidato"),
        ct.zero_pad_code(F.col("municipio_ibge").cast("string"), 7).alias("municipio_ibge"),
        ct.standardize_uf(F.col("uf")).alias("uf"),
        F.col("zona").cast("string").alias("zona"),
        ct.coalesce_zero(F.col("votos").cast("int")).alias("votos"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze tse_votacao: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
