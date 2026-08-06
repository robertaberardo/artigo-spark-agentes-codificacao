from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
KRING_K = 1
METERS_PER_KM = 1_000.0
DIST_DECIMALS = 3
ENROLLMENT_DECIMALS = 2

# ponto (lat/long WGS84) usado para o H3 e para a distância geodésica; as
# medidas de distância são feitas sobre estas coordenadas originais, nunca
# sobre a projeção 3857 (só a geometria de saída é reprojetada).
POINT_GEOM = "point"


def prepare_schools(df: DataFrame) -> DataFrame:
    """Adiciona o ponto WGS84 e a célula H3 (resolução 5) de cada escola."""
    with_point = dt.add_point_geometry(df, "latitude", "longitude", POINT_GEOM)
    return dt.attach_h3_index(with_point, POINT_GEOM, H3_LEVEL, "h3_cell").select(
        F.col("id_unidade"),
        F.col("enrollment"),
        F.col("h3_cell"),
        F.col(POINT_GEOM),
    )


def neighbor_membership(schools: DataFrame) -> DataFrame:
    """Mapeia cada escola às células cuja vizinhança (kRing 1) a contém.

    ``ST_H3KRing(cell, 1, False)`` devolve a célula e as 6 adjacentes. Como o
    kRing de distância 1 é simétrico, explodi-lo dá, para cada célula-alvo, todas
    as escolas situadas nela ou numa célula imediatamente adjacente.
    """
    ring = stf.ST_H3KRing(F.col("h3_cell"), F.lit(KRING_K), F.lit(False))
    return schools.select(
        F.col("id_unidade").alias("nb_id"),
        F.col(POINT_GEOM).alias("nb_point"),
        F.explode(ring).alias("target_cell"),
    )


def neighborhood_count(membership: DataFrame) -> DataFrame:
    """Conta as escolas vizinhas por célula (tamanho da vizinhança menos a própria).

    O valor é o mesmo para toda a célula: quantas outras federais em atividade
    estão na célula ou nas adjacentes.
    """
    return membership.groupBy(F.col("target_cell")).agg(
        (F.count(F.lit(1)) - F.lit(1)).cast("long").alias("n_federal_neighbors")
    ).select(
        F.col("target_cell").alias("h3_cell"),
        F.col("n_federal_neighbors"),
    )


def distance_km(geom_a: str, geom_b: str) -> F.Column:
    """Distância geodésica (km) entre dois pontos WGS84 (``ST_DistanceSpheroid``)."""
    return stf.ST_DistanceSpheroid(F.col(geom_a), F.col(geom_b)) / F.lit(METERS_PER_KM)


def neighbor_distance_per_school(schools: DataFrame, membership: DataFrame) -> DataFrame:
    """Distância média de cada escola às suas vizinhas (mesma célula ou adjacentes)."""
    base = schools.select(
        F.col("id_unidade").alias("s_id"),
        F.col("h3_cell").alias("s_cell"),
        F.col(POINT_GEOM).alias("s_point"),
    )
    pairs = base.join(
        membership, base["s_cell"] == membership["target_cell"], how="inner"
    ).filter(F.col("s_id") != F.col("nb_id"))
    return pairs.groupBy(F.col("s_id")).agg(
        F.avg(distance_km("s_point", "nb_point")).alias("avg_dist_neighbors_km")
    ).select(
        F.col("s_id").alias("id_unidade"),
        F.col("avg_dist_neighbors_km"),
    )


def nearest_federal_per_school(schools: DataFrame) -> DataFrame:
    """Distância de cada escola à federal em atividade mais próxima (sem restrição de célula)."""
    left = schools.select(
        F.col("id_unidade").alias("a_id"), F.col(POINT_GEOM).alias("a_point")
    )
    right = schools.select(
        F.col("id_unidade").alias("b_id"), F.col(POINT_GEOM).alias("b_point")
    )
    pairs = left.crossJoin(F.broadcast(right)).filter(F.col("a_id") != F.col("b_id"))
    return pairs.groupBy(F.col("a_id")).agg(
        F.min(distance_km("a_point", "b_point")).alias("dist_nearest_km")
    ).select(
        F.col("a_id").alias("id_unidade"),
        F.col("dist_nearest_km"),
    )


def aggregate_cells(enriched: DataFrame, neighbors: DataFrame) -> DataFrame:
    """Agrega as métricas por célula H3 e anexa a contagem de vizinhos."""
    cells = enriched.groupBy(F.col("h3_cell")).agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum(F.col("enrollment")).cast("long").alias("total_enrollment"),
        F.round(F.avg(F.col("enrollment")), ENROLLMENT_DECIMALS).alias("avg_enrollment"),
        F.round(F.avg(F.col("avg_dist_neighbors_km")), DIST_DECIMALS).alias(
            "avg_dist_neighbors_km"
        ),
        F.round(F.avg(F.col("dist_nearest_km")), DIST_DECIMALS).alias(
            "avg_dist_nearest_km"
        ),
    )
    return dt.join_lookup(cells, neighbors, on="h3_cell", how="left", broadcast=True)


def add_cell_geometry(df: DataFrame) -> DataFrame:
    """Reconstrói o polígono da célula H3 (WGS84) e o reprojeta para EPSG:3857."""
    cell_geom = F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
    return dt.to_web_mercator(df.withColumn("geometry", cell_geom))


def main():
    """Grade H3 (res. 5) das escolas federais em atividade: densidade, matrícula e vizinhança."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = prepare_schools(spark.read.parquet(ORIGEM)).cache()

    membership = neighbor_membership(schools)
    per_school_neighbors = neighbor_distance_per_school(schools, membership)
    per_school_nearest = nearest_federal_per_school(schools)
    neighbors = neighborhood_count(membership)

    enriched = (
        schools.select(F.col("id_unidade"), F.col("h3_cell"), F.col("enrollment"))
        .join(per_school_neighbors, on="id_unidade", how="left")
        .join(per_school_nearest, on="id_unidade", how="left")
    )

    grid = aggregate_cells(enriched, neighbors).transform(add_cell_geometry).select(
        F.col("h3_cell"),
        F.col("n_schools"),
        F.col("total_enrollment"),
        F.col("avg_enrollment"),
        F.col("n_federal_neighbors"),
        F.col("avg_dist_neighbors_km"),
        F.col("avg_dist_nearest_km"),
        F.col("geometry"),
    )

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    grid.orderBy(F.col("n_schools").desc()).show(10, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
