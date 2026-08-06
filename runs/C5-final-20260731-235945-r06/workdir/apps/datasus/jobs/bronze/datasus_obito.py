from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.datasus.utils import column_transforms as datasus_ct
from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("datasus", "obito", "raw")
DESTINO = table_location("datasus", "obito", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("municipio_ibge", StringType()),
        StructField("uf", StringType()),
        StructField("data_obito", StringType()),
        StructField("sexo", StringType()),
        StructField("idade", StringType()),
        StructField("causa_cid", StringType()),
    ]
)


def main():
    """Lê o raw dos óbitos, tipa data e idade e salva a bronze."""
    spark = SparkSession.builder.appName("datasus-obito-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.trim(F.col("id")).alias("id"),
        ct.digits_only(F.col("municipio_ibge")).alias("municipio_ibge"),
        ct.standardize_uf(F.col("uf")).alias("uf"),
        ct.parse_br_date(F.col("data_obito")).alias("data_obito"),
        F.upper(F.trim(F.col("sexo"))).alias("sexo"),
        F.col("idade").cast("int").alias("idade"),
        F.upper(F.trim(F.col("causa_cid"))).alias("causa_cid"),
    ).filter(datasus_ct.is_valid_cid(F.col("causa_cid")))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze datasus_obito: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
