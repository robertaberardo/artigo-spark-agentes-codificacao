from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula", "silver")

# Códigos do Censo Escolar: setor 1 = federal; situação 1 = em atividade.
SETOR_FEDERAL = "1"
SITUACAO_ATIVA = "1"


def keep_federal_active(df):
    """Mantém só escolas federais em atividade — universo de toda a análise."""
    is_federal = F.col("setor") == SETOR_FEDERAL
    is_active = F.col("situacao") == SITUACAO_ATIVA
    return df.filter(is_federal & is_active)


def main():
    """Consolida escola + matrícula (federais em atividade) e georreferencia.

    Preserva latitude/longitude WGS84 originais além da geometria em 3857: o gold
    mede distâncias geodésicas sobre o ponto original e indexa o H3 em coordenadas
    geográficas, sem depender do round-trip da projeção.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA)

    consolidado = (
        escola.transform(keep_federal_active)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .transform(
            lambda d: dt.join_lookup(
                d, matricula, on="id_unidade", columns=["mat_basica"], how="left"
            )
        )
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "nome_unidade",
            "uf",
            "cod_municipio",
            "setor",
            "situacao",
            "latitude",
            "longitude",
            F.col("mat_basica").alias("total_enrollment"),
            "geometry",
        )
    )

    consolidado.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_matricula: {n} escolas federais ativas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
