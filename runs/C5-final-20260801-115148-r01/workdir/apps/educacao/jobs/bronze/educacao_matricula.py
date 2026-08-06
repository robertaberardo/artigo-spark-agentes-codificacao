from pyspark.sql import SparkSession

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

# Colunas de contagem de matrículas, na ordem do contrato bronze.
CONTAGENS = [
    "mat_basica",
    "mat_infantil",
    "mat_fundamental",
    "mat_medio",
    "mat_profissional",
    "mat_eja",
    "mat_especial",
]


def main():
    """Lê o raw das matrículas, tipa as contagens como int e salva a bronze."""
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = spark.read.option("header", True).option("sep", ";").csv(ORIGEM)

    bronze = raw.select(
        ct.zero_pad_code(raw["ID_UNIDADE"], 8).alias("id_unidade"),
        *[raw[c.upper()].cast("int").alias(c) for c in CONTAGENS],
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
