from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
NEIGHBOR_RING = 1  # k-anel 1: própria célula + células imediatamente adjacentes
FEDERAL_SETOR = "1"
SITUACAO_ATIVA = "1"
METERS_PER_KM = 1000.0


def select_active_federal_schools(df: DataFrame) -> DataFrame:
    """Mantém apenas as escolas federais em atividade e indexa por célula H3.

    Constrói o ponto WGS84 (``point_wgs84``) a partir das coordenadas originais —
    base tanto do índice H3 quanto das distâncias geodésicas — e anexa a célula H3
    do nível pedido.
    """
    is_federal = F.col("setor") == FEDERAL_SETOR
    is_active = F.col("situacao") == SITUACAO_ATIVA
    return (
        df.filter(is_federal & is_active)
        .transform(
            lambda d: dt.add_point_geometry(d, "latitude", "longitude", "point_wgs84")
        )
        .transform(lambda d: dt.attach_h3_index(d, "point_wgs84", H3_LEVEL, "h3_cell"))
    )


def enrich_with_distance_stats(federal: DataFrame) -> DataFrame:
    """Anexa a cada escola as estatísticas de vizinhança e da federal mais próxima.

    Vizinhas são as outras federais ativas na mesma célula ou em célula adjacente
    (k-anel ``NEIGHBOR_RING``); ``n_neighbors`` é idêntico para toda a célula.
    Escolas sem vizinha recebem contagem 0 (ausência de vizinha conta como zero),
    mas mantêm a distância média de vizinhas em ``NULL`` — ausência não é zero.
    """
    neighbors = dt.h3_neighbor_distance_stats(
        federal, "id_unidade", "h3_cell", "point_wgs84", NEIGHBOR_RING
    )
    nearest = dt.nearest_point_distance(federal, "id_unidade", "point_wgs84")
    return (
        federal.select("id_unidade", "h3_cell", "total_enrollment")
        .join(neighbors, "id_unidade", "left")
        .join(nearest, "id_unidade", "left")
        .withColumn("n_neighbors", F.coalesce(F.col("n_neighbors"), F.lit(0)))
    )


def main():
    """Grade H3 nível 5 das escolas federais em atividade, com métricas de vizinhança."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("geoparquet").load(ORIGEM)

    federal = select_active_federal_schools(silver)
    enriched = enrich_with_distance_stats(federal)

    grid_aggregations = [
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("total_enrollment").cast("long").alias("total_enrollment"),
        F.round(F.avg("total_enrollment"), 2).alias("avg_enrollment"),
        # constante por célula: basta um valor qualquer do grupo
        F.max("n_neighbors").cast("long").alias("n_federal_neighbors"),
        F.round(F.avg("avg_dist_neighbors_m") / F.lit(METERS_PER_KM), 3).alias(
            "avg_dist_neighbors_km"
        ),
        F.round(F.avg("dist_nearest_m") / F.lit(METERS_PER_KM), 3).alias(
            "avg_dist_nearest_km"
        ),
    ]

    grid = (
        dt.aggregate_by_h3(enriched, "h3_cell", grid_aggregations)
        # ST_H3ToGeom devolve o polígono em EPSG:4326; persiste-se em 3857 (convenção)
        .transform(dt.to_web_mercator)
        .select(
            "h3_cell",
            "n_schools",
            "total_enrollment",
            "avg_enrollment",
            "n_federal_neighbors",
            "avg_dist_neighbors_km",
            "avg_dist_nearest_km",
            "geometry",
        )
    )

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
