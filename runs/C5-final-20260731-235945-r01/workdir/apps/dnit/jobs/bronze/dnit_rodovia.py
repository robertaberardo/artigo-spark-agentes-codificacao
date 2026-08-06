from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.dnit.utils import column_transforms as dnit_ct
from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("dnit", "rodovia", "raw")
DESTINO = table_location("dnit", "rodovia", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("sigla_br", StringType()),
        StructField("uf", StringType()),
        StructField("extensao_km", StringType()),
        StructField("geom_wkt", StringType()),
    ]
)


def main():
    """Lê o raw das rodovias, padroniza a sigla e a extensão e salva a bronze."""
    spark = SparkSession.builder.appName("dnit-rodovia-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        dnit_ct.normalize_br(F.col("sigla_br")).alias("sigla_br"),
        ct.standardize_uf(F.col("uf")).alias("uf"),
        ct.br_decimal_to_double(F.col("extensao_km")).alias("extensao_km"),
        ct.normalize_text(F.col("geom_wkt")).alias("geom_wkt"),
    ).filter(dnit_ct.is_valid_br(F.col("sigla_br")))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze dnit_rodovia: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
