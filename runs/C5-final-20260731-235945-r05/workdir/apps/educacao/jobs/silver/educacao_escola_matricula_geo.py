from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula_geo", "silver")

# Filtros de recorte: setor 1 = federal, situação 1 = em atividade.
SETOR_FEDERAL = "1"
SITUACAO_ATIVIDADE = "1"

# Nível da grade H3 pedido para a agregação a jusante.
H3_LEVEL = 5


def main():
    """Escolas federais em atividade, georreferenciadas e com matrículas.

    Recorta o cadastro (federal + em atividade), constrói o ponto WGS84, indexa
    a célula H3 nível 5 sobre o ponto original e enriquece com o total de
    matrículas. Preserva lat/long em ``double`` para a distância geodésica no
    gold e persiste a geometria em EPSG:3857.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA).filter(
        (F.col("setor") == SETOR_FEDERAL) & (F.col("situacao") == SITUACAO_ATIVIDADE)
    )
    matricula = spark.read.parquet(MATRICULA).select(
        F.col("id_unidade"),
        F.col("mat_basica").alias("total_enrollment"),
    )

    geo = (
        escola.transform(
            lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude")
        )
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        # H3 é calculado sobre o ponto WGS84 ORIGINAL, antes de projetar.
        .transform(lambda d: dt.attach_h3_index(d, "geometry", H3_LEVEL, "h3_cell"))
        .transform(dt.to_web_mercator)
        .transform(
            lambda d: dt.join_no_fanout(d, matricula, "id_unidade", "left")
        )
        .select(
            "id_unidade",
            "uf",
            "latitude",
            "longitude",
            "total_enrollment",
            "h3_cell",
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_matricula_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
