from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils import dataframe_transforms as dt

ADMISSAO = "s3a://datalake/caged/caged_admissao/bronze"
DESLIGAMENTO = "s3a://datalake/caged/caged_desligamento/bronze"
DESTINO = "s3a://datalake/caged/caged_movimentacao_consolidada/silver"


def main():
    """Une admissões e desligamentos rotulando o tipo de cada movimentação."""
    spark = SparkSession.builder.appName("caged-movimentacao-consolidada-silver").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    admissao = spark.read.parquet(ADMISSAO)
    desligamento = spark.read.parquet(DESLIGAMENTO)

    admissoes = admissao.select(
        admissao["id"],
        F.lit("admissao").alias("movement_type"),
        admissao["municipio_ibge"],
        admissao["uf"],
        admissao["cbo"],
        admissao["cnae"],
        admissao["data"],
        admissao["salario"],
    )

    desligamentos = desligamento.select(
        desligamento["id"],
        F.lit("desligamento").alias("movement_type"),
        desligamento["municipio_ibge"],
        desligamento["uf"],
        desligamento["cbo"],
        desligamento["cnae"],
        desligamento["data"],
        desligamento["salario"],
    )

    consolidada = dt.union_dataframes([admissoes, desligamentos])

    silver = consolidada.select(
        "id",
        "movement_type",
        "municipio_ibge",
        "uf",
        "cbo",
        "cnae",
        "data",
        "salario",
    )

    silver.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> silver caged_movimentacao_consolidada: {n} movimentações em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
