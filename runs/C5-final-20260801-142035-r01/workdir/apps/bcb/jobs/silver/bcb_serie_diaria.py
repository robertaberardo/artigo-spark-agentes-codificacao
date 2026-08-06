from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils import dataframe_transforms as dt

CAMBIO = "s3a://datalake/bcb/bcb_cambio/raw"
SELIC = "s3a://datalake/bcb/bcb_selic/bronze"
DESTINO = "s3a://datalake/bcb/bcb_serie_diaria/silver"

CAMBIO_SCHEMA = StructType(
    [
        StructField("data", StringType()),
        StructField("moeda", StringType()),
        StructField("compra", StringType()),
        StructField("venda", StringType()),
    ]
)

PERIODO_INICIO = "2020-01-01"
PERIODO_FIM = "2025-12-31"
JANELA_MEDIA_MOVEL = 7


def main():
    """Série diária de câmbio enriquecida com a Selic, média móvel e acumulado."""
    spark = SparkSession.builder.appName("bcb-serie-diaria-silver").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    cambio = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(CAMBIO_SCHEMA)
        .csv(CAMBIO)
        .select(
            ct.parse_br_date(F.col("data")).alias("data"),
            F.upper(F.trim(F.col("moeda"))).alias("moeda"),
            ct.br_decimal_to_double(F.col("compra")).alias("compra"),
            ct.br_decimal_to_double(F.col("venda")).alias("venda"),
        )
    )

    selic = spark.read.parquet(SELIC).select(
        F.col("data"),
        F.col("taxa_meta_pct").alias("selic_meta"),
        F.col("taxa_efetiva_pct").alias("selic_efetiva"),
    )

    serie = (
        dt.join_no_fanout(cambio, selic, on="data", how="left")
        .withColumn("spread", F.col("venda") - F.col("compra"))
        .transform(
            lambda d: dt.filter_by_date_range(d, "data", PERIODO_INICIO, PERIODO_FIM)
        )
        .transform(
            lambda d: dt.add_moving_average(
                d, "venda", "data", JANELA_MEDIA_MOVEL,
                out_col="venda_ma7", partition_cols=["moeda"],
            )
        )
        .transform(
            lambda d: dt.add_running_total(
                d, "spread", "data",
                out_col="spread_acumulado", partition_cols=["moeda"],
            )
        )
        .select(
            "data",
            "moeda",
            "compra",
            "venda",
            "spread",
            "selic_meta",
            "selic_efetiva",
            "venda_ma7",
            "spread_acumulado",
        )
    )

    serie.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> silver bcb_serie_diaria: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
