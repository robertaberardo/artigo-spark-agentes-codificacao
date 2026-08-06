from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("ana", "bacia", "raw")
DESTINO = table_location("ana", "bacia", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("codigo_bacia", StringType()),
        StructField("nome", StringType()),
        StructField("geom_wkt", StringType()),
    ]
)


def main():
    """Lê o raw das bacias e salva a bronze mantendo o WKT como texto."""
    spark = SparkSession.builder.appName("ana-bacia-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.upper(F.trim(F.col("codigo_bacia"))).alias("codigo_bacia"),
        ct.normalize_text(F.col("nome")).alias("nome"),
        F.trim(F.col("geom_wkt")).alias("geom_wkt"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze ana_bacia: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
