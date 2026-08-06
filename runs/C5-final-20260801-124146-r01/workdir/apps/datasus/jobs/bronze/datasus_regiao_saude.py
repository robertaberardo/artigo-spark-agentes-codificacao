from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("datasus", "regiao_saude", "raw")
DESTINO = table_location("datasus", "regiao_saude", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("co_regiao", StringType()),
        StructField("nome", StringType()),
        StructField("uf", StringType()),
        StructField("geom_wkt", StringType()),
    ]
)


def main():
    """Lê o raw das regiões de saúde, normaliza nome/UF e mantém o WKT na bronze."""
    spark = SparkSession.builder.appName("datasus-regiao-saude-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.upper(F.trim(F.col("co_regiao"))).alias("co_regiao"),
        ct.normalize_text(F.col("nome")).alias("nome"),
        ct.standardize_uf(F.col("uf")).alias("uf"),
        F.trim(F.col("geom_wkt")).alias("geom_wkt"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze datasus_regiao_saude: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
