from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula", "silver")

# setor administrativo e situação de funcionamento relevantes (metadata.toml)
SETOR_FEDERAL = 1
SITUACAO_ATIVA = 1


def keep_federal_active(df: DataFrame) -> DataFrame:
    """Mantém apenas escolas do setor federal e em atividade."""
    is_federal = F.col("setor") == SETOR_FEDERAL
    is_active = F.col("situacao") == SITUACAO_ATIVA
    return df.filter(is_federal & is_active)


def attach_enrollment(escola: DataFrame, matricula: DataFrame) -> DataFrame:
    """Anexa as matrículas da educação básica à escola (join por id_unidade).

    ``MAT_BASICA`` é o total da educação básica; usa-se ``join_no_fanout`` porque a
    matrícula deve ser única por unidade — duplicidade viraria erro, não fan-out.
    """
    enrollment = matricula.select(
        F.col("id_unidade"), F.col("mat_basica").alias("enrollment")
    )
    return dt.join_no_fanout(escola, enrollment, on="id_unidade", how="left")


def main():
    """Escolas federais em atividade, georreferenciadas e com matrículas anexadas."""
    spark = SparkSession.builder.appName("educacao-escola-matricula-silver").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA)

    federal = (
        escola.transform(keep_federal_active)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
    )

    silver = attach_enrollment(federal, matricula).select(
        F.col("id_unidade"),
        F.col("nome_unidade"),
        F.col("uf"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("enrollment"),
    )

    silver.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> silver educacao_escola_matricula: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
