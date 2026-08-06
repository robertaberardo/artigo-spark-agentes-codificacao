from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_geo", "silver")

SETOR_FEDERAL = 1
SITUACAO_ATIVA = 1
H3_LEVEL = 5


def filter_federal_ativa(df: DataFrame) -> DataFrame:
    """Mantém apenas escolas do setor federal e em atividade."""
    return df.filter(
        (F.col("setor") == SETOR_FEDERAL) & (F.col("situacao") == SITUACAO_ATIVA)
    )


def main():
    """Escolas federais em atividade georreferenciadas: ponto, célula H3 e matrículas."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA)

    geo = (
        escola.transform(filter_federal_ativa)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        # left join: preserva escolas sem registro de matrícula (ausência ≠ zero)
        .transform(lambda d: dt.join_no_fanout(d, matricula, "id_unidade", how="left"))
        .withColumnRenamed("mat_basica", "enrollment")
        # célula H3 é calculada sobre o ponto em WGS84, antes de projetar
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(lambda d: dt.attach_h3_index(d, "geometry", H3_LEVEL, "h3_cell"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "nome_unidade",
            "uf",
            "latitude",
            "longitude",
            "enrollment",
            "h3_cell",
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
