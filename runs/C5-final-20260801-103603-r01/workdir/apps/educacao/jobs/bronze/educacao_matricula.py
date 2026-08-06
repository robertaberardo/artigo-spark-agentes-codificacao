from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

# Schema do raw em ordem posicional (todo texto). Só a chave e o total da
# educação básica seguem para a bronze; os demais recortes não são usados aqui.
RAW_SCHEMA = StructType(
    [
        StructField("id_unidade", StringType()),
        StructField("mat_basica", StringType()),
        StructField("mat_infantil", StringType()),
        StructField("mat_fundamental", StringType()),
        StructField("mat_medio", StringType()),
        StructField("mat_profissional", StringType()),
        StructField("mat_eja", StringType()),
        StructField("mat_especial", StringType()),
    ]
)


def main():
    """Lê o raw das matrículas, padroniza chave/total e salva a bronze."""
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(raw["id_unidade"], 8).alias("id_unidade"),
        raw["mat_basica"].cast("int").alias("mat_basica"),
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
