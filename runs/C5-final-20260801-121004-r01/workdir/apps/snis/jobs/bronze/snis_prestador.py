from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("snis", "prestador", "raw")
DESTINO = table_location("snis", "prestador", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("codigo", StringType()),
        StructField("nome", StringType()),
        StructField("uf", StringType()),
        StructField("natureza_juridica", StringType()),
        StructField("abrangencia", StringType()),
    ]
)


def main():
    """Lê o raw dos prestadores, normaliza nome e UF e salva a bronze."""
    spark = SparkSession.builder.appName("snis-prestador-bronze").getOrCreate()
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
        ct.normalize_text(raw["natureza_juridica"]).alias("natureza_juridica"),
        ct.normalize_text(raw["abrangencia"]).alias("abrangencia"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze snis_prestador: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
