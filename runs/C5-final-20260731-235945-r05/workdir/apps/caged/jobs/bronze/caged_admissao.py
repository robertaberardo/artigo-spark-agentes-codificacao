from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.caged.utils import column_transforms as caged_ct
from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("caged", "admissao", "raw")
DESTINO = table_location("caged", "admissao", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("municipio_ibge", StringType()),
        StructField("uf", StringType()),
        StructField("cbo", StringType()),
        StructField("cnae", StringType()),
        StructField("data", StringType()),
        StructField("salario", StringType()),
        StructField("sexo", StringType()),
    ]
)


def main():
    """Lê o raw das admissões, tipa as colunas e salva a bronze."""
    spark = SparkSession.builder.appName("caged-admissao-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.trim(F.col("id")).alias("id"),
        ct.zero_pad_code(F.col("municipio_ibge"), 7).alias("municipio_ibge"),
        ct.standardize_uf(F.col("uf")).alias("uf"),
        ct.zero_pad_code(F.col("cbo"), 6).alias("cbo"),
        ct.zero_pad_code(F.col("cnae"), 7).alias("cnae"),
        ct.parse_br_date(F.col("data")).alias("data"),
        ct.money_br_to_double(F.col("salario")).alias("salario"),
        ct.strip_accents_lower(F.col("sexo")).alias("sexo"),
    ).filter(caged_ct.is_valid_cbo(F.col("cbo")))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze caged_admissao: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
