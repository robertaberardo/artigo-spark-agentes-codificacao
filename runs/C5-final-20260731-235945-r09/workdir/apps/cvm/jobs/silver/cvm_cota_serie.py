from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils import dataframe_transforms as dt

COTA = "s3a://datalake/cvm/cvm_cota/raw"
INFORME = "s3a://datalake/cvm/cvm_informe_diario/raw"
DESTINO = "s3a://datalake/cvm/cvm_cota_serie/silver"

COTA_SCHEMA = StructType(
    [
        StructField("cnpj_fundo", StringType()),
        StructField("data", StringType()),
        StructField("valor_cota", StringType()),
        StructField("patrimonio_liquido", StringType()),
        StructField("cotistas", StringType()),
    ]
)

INFORME_SCHEMA = StructType(
    [
        StructField("cnpj_fundo", StringType()),
        StructField("data", StringType()),
        StructField("captacao", StringType()),
        StructField("resgate", StringType()),
    ]
)


def _read_csv(spark, path, schema):
    """Lê um CSV cru com separador ``;`` e o schema informado."""
    return (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(schema)
        .csv(path)
    )


def main():
    """Monta a série temporal de cotas por fundo, com média móvel e captação acumulada."""
    spark = SparkSession.builder.appName("cvm-cota-serie-silver").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    cota = _read_csv(spark, COTA, COTA_SCHEMA).select(
        F.upper(F.trim(F.col("cnpj_fundo"))).alias("cnpj_fundo"),
        ct.parse_br_date(F.col("data")).alias("data"),
        ct.br_decimal_to_double(F.col("valor_cota")).alias("valor_cota"),
        ct.money_br_to_double(F.col("patrimonio_liquido")).alias("patrimonio_liquido"),
        F.col("cotistas").cast("long").alias("cotistas"),
    )

    informe = _read_csv(spark, INFORME, INFORME_SCHEMA).select(
        F.upper(F.trim(F.col("cnpj_fundo"))).alias("cnpj_fundo"),
        ct.parse_br_date(F.col("data")).alias("data"),
        ct.money_br_to_double(F.col("captacao")).alias("captacao"),
    )

    serie = (
        dt.join_no_fanout(cota, informe, on=["cnpj_fundo", "data"], how="left")
        .withColumn("captacao", ct.coalesce_zero(F.col("captacao")))
        .transform(
            lambda d: dt.add_moving_average(
                d,
                value_col="valor_cota",
                order_col="data",
                window_size=3,
                out_col="valor_cota_mm",
                partition_cols=["cnpj_fundo"],
            )
        )
        .transform(
            lambda d: dt.add_running_total(
                d,
                value_col="captacao",
                order_col="data",
                out_col="captacao_acumulada",
                partition_cols=["cnpj_fundo"],
            )
        )
        .select(
            "cnpj_fundo",
            "data",
            "valor_cota",
            "patrimonio_liquido",
            "cotistas",
            "captacao",
            "valor_cota_mm",
            "captacao_acumulada",
        )
    )

    serie.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> silver cvm_cota_serie: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
