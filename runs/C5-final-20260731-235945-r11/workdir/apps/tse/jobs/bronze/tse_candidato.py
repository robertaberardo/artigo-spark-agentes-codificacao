from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.tse.utils import column_transforms as tse_ct
from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("tse", "candidato", "raw")
DESTINO = table_location("tse", "candidato", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("id_candidato", StringType()),
        StructField("nome", StringType()),
        StructField("partido_sigla", StringType()),
        StructField("uf", StringType()),
        StructField("cargo", StringType()),
        StructField("situacao", StringType()),
        StructField("votos", StringType()),
    ]
)


def main():
    """Read the raw candidates, type the columns and write the bronze table."""
    spark = SparkSession.builder.appName("tse-candidato-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.upper(F.trim(F.col("id_candidato"))).alias("id_candidato"),
        ct.normalize_text(F.col("nome")).alias("nome"),
        tse_ct.normalize_party_acronym(F.col("partido_sigla")).alias("partido_sigla"),
        ct.standardize_uf(F.col("uf")).alias("uf"),
        ct.normalize_text(F.col("cargo")).alias("cargo"),
        ct.blank_to_null(F.col("situacao")).alias("situacao"),
        ct.coalesce_zero(F.col("votos").cast("int")).alias("votos"),
    ).filter(tse_ct.is_valid_party_acronym(F.col("partido_sigla")))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze tse_candidato: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
