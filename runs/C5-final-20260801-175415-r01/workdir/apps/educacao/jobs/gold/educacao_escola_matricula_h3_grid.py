from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_federal_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
KM = 1_000.0


def load_schools(spark) -> DataFrame:
    """Escolas federais com ponto WGS84 (para geodésica) e célula H3 nível 5."""
    return (
        spark.read.format("geoparquet")
        .load(ORIGEM)
        .select("id_unidade", "total_enrollment", "latitude", "longitude")
        .transform(
            lambda d: dt.add_point_geometry(d, "latitude", "longitude", "geom_wgs84")
        )
        .transform(lambda d: dt.attach_h3_index(d, "geom_wgs84", H3_LEVEL, "h3_cell"))
    )


def cell_base(schools: DataFrame) -> DataFrame:
    """Contagem, total e média de matrículas por célula, com geometria da célula."""
    return schools.transform(
        lambda d: dt.aggregate_by_h3(
            d,
            "h3_cell",
            [
                F.count(F.lit(1)).cast("long").alias("n_schools"),
                F.sum("total_enrollment").cast("long").alias("total_enrollment"),
                F.avg("total_enrollment").alias("avg_enrollment"),
            ],
        )
    )


def neighbor_distances_by_cell(schools: DataFrame) -> DataFrame:
    """Média por célula da distância média de cada escola às suas vizinhas (km)."""
    return (
        schools.transform(
            lambda d: dt.h3_kring_neighbor_distances(
                d, "id_unidade", "h3_cell", "geom_wgs84", 1, "dist_neighbors_m"
            )
        )
        .groupBy("h3_cell")
        .agg(F.avg("dist_neighbors_m").alias("_dist_neighbors_m"))
    )


def nearest_distances_by_cell(schools: DataFrame) -> DataFrame:
    """Média por célula da distância de cada escola à federal mais próxima (km)."""
    nearest = schools.transform(
        lambda d: dt.nearest_neighbor_distance(d, "id_unidade", "geom_wgs84")
    )
    cells = schools.select("id_unidade", "h3_cell")
    return (
        nearest.join(cells, "id_unidade", "left")
        .groupBy("h3_cell")
        .agg(F.avg("dist_nearest_m").alias("_dist_nearest_m"))
    )


def main():
    """Grade H3 nível 5 das escolas federais em atividade: densidade, matrículas,
    vizinhança e distâncias médias (geodésicas) às vizinhas e à mais próxima."""
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = load_schools(spark).cache()

    base = cell_base(schools)
    pool = dt.count_h3_kring_members(schools, "h3_cell", 1, "_pool_size")
    neighbors = neighbor_distances_by_cell(schools)
    nearest = nearest_distances_by_cell(schools)

    grid = (
        base.join(pool, "h3_cell", "left")
        .join(neighbors, "h3_cell", "left")
        .join(nearest, "h3_cell", "left")
        .transform(dt.to_web_mercator)
        .select(
            "h3_cell",
            "n_schools",
            "total_enrollment",
            F.col("avg_enrollment").cast("double").alias("avg_enrollment"),
            (F.col("_pool_size") - F.lit(1)).cast("long").alias("n_federal_neighbors"),
            (F.col("_dist_neighbors_m") / F.lit(KM)).alias("avg_dist_neighbors_km"),
            (F.col("_dist_nearest_m") / F.lit(KM)).alias("avg_dist_nearest_km"),
            "geometry",
        )
    )

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
