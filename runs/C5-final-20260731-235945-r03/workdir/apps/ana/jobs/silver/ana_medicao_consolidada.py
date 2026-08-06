from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("ana", "medicao_fluvio", "raw")
ESTACAO = table_location("ana", "estacao", "bronze")
DESTINO = table_location("ana", "medicao_consolidada", "silver")

DATA_INICIO, DATA_FIM = "2015-01-01", "2024-12-31"
MEDIA_MOVEL_MEDICOES = 7

RAW_SCHEMA = StructType(
    [
        StructField("codigo_estacao", StringType()),
        StructField("data", StringType()),
        StructField("nivel_cm", StringType()),
        StructField("vazao_m3s", StringType()),
        StructField("chuva_mm", StringType()),
    ]
)


def main():
    """Medições com UF/rio da estação, recorte de período e média móvel de vazão."""
    spark = SparkSession.builder.appName("ana-medicao-consolidada-silver").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    medicao = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
        .select(
            F.upper(F.trim(F.col("codigo_estacao"))).alias("codigo_estacao"),
            ct.parse_br_date(F.col("data")).alias("data"),
            ct.br_decimal_to_double(F.col("nivel_cm")).alias("nivel_cm"),
            ct.br_decimal_to_double(F.col("vazao_m3s")).alias("vazao_m3s"),
            ct.br_decimal_to_double(F.col("chuva_mm")).alias("chuva_mm"),
        )
    )

    estacao = spark.read.parquet(ESTACAO).select(
        F.col("codigo").alias("codigo_estacao"), F.col("uf"), F.col("rio")
    )

    result = (
        medicao.transform(lambda d: dt.join_lookup(d, estacao, "codigo_estacao"))
        .transform(lambda d: dt.filter_by_date_range(d, "data", DATA_INICIO, DATA_FIM))
        .transform(
            lambda d: dt.add_moving_average(
                d,
                "vazao_m3s",
                "data",
                MEDIA_MOVEL_MEDICOES,
                out_col="vazao_moving_avg",
                partition_cols=["codigo_estacao"],
            )
        )
        .select(
            "codigo_estacao",
            "data",
            "uf",
            "rio",
            "nivel_cm",
            "vazao_m3s",
            "chuva_mm",
            "vazao_moving_avg",
        )
    )

    result.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> silver ana_medicao_consolidada: {n} medições em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
