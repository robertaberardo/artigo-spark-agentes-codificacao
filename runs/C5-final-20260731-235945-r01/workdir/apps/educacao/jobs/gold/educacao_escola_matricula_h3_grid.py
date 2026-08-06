from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
# vizinhança = própria célula + células imediatamente adjacentes (anel k=1)
NEIGHBOR_RINGS = 1
METERS_PER_KM = 1000.0


def prepare_schools(silver: DataFrame) -> DataFrame:
    """Constrói o ponto WGS84 e a célula H3 de cada escola.

    A distância geodésica é medida sobre a coordenada WGS84 original, então o
    ponto é reconstruído a partir de ``longitude``/``latitude`` (sem reprojetar).
    """
    return (
        silver.withColumn("geom_wgs84", stc.ST_Point(F.col("longitude"), F.col("latitude")))
        .withColumn(
            "h3_cell",
            F.element_at(stf.ST_H3CellIDs(F.col("geom_wgs84"), H3_LEVEL, False), 1),
        )
        .select("id_unidade", "total_enrollment", "h3_cell", "geom_wgs84")
    )


def cell_base_aggregates(schools: DataFrame) -> DataFrame:
    """Contagem, total e média de matrículas por célula.

    A média ignora escolas sem matrícula registrada (ausência não é zero).
    """
    return schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("total_enrollment").cast("long").alias("total_enrollment"),
        F.avg("total_enrollment").alias("avg_enrollment"),
    )


def geodesic_km(geom_a: str, geom_b: str) -> Column:
    """Distância geodésica (WGS84) entre dois pontos, em quilômetros."""
    return stf.ST_DistanceSpheroid(F.col(geom_a), F.col(geom_b)) / F.lit(METERS_PER_KM)


def cell_neighbor_distances(schools: DataFrame) -> DataFrame:
    """Distância média às vizinhas, agregada por célula.

    Vizinhas de uma escola são as demais escolas na própria célula ou nas
    adjacentes (anel k=1). Calcula a média por escola e depois a média por célula.
    """
    origin = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("geom_wgs84").alias("a_geom"),
    ).withColumn(
        "search_cell",
        F.explode(stf.ST_H3KRing(F.col("a_cell"), NEIGHBOR_RINGS, False)),
    )
    target = schools.select(
        F.col("id_unidade").alias("b_id"),
        F.col("h3_cell").alias("b_cell"),
        F.col("geom_wgs84").alias("b_geom"),
    )

    pairs = (
        origin.join(target, F.col("search_cell") == F.col("b_cell"), "inner")
        .filter(F.col("a_id") != F.col("b_id"))
        .withColumn("dist_km", geodesic_km("a_geom", "b_geom"))
    )
    per_school = pairs.groupBy("a_id", "a_cell").agg(
        F.avg("dist_km").alias("school_mean_km")
    )
    return per_school.groupBy("a_cell").agg(
        F.avg("school_mean_km").alias("avg_dist_neighbors_km")
    ).select(F.col("a_cell").alias("h3_cell"), "avg_dist_neighbors_km")


def cell_neighbor_counts(schools: DataFrame) -> DataFrame:
    """Nº de escolas federais na própria célula e nas adjacentes.

    É uma propriedade da célula (igual para todas as suas escolas): a soma das
    escolas de cada célula do anel k=1, incluindo a própria.
    """
    per_cell = schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).alias("cell_count")
    )
    disk = schools.select("h3_cell").distinct().withColumn(
        "member_cell", F.explode(stf.ST_H3KRing(F.col("h3_cell"), NEIGHBOR_RINGS, False))
    )
    members = per_cell.select(
        F.col("h3_cell").alias("member_cell"), "cell_count"
    )
    return (
        disk.join(members, on="member_cell", how="left")
        .groupBy("h3_cell")
        .agg(F.sum("cell_count").cast("long").alias("n_federal_neighbors"))
    )


def cell_nearest_distances(schools: DataFrame) -> DataFrame:
    """Distância à federal mais próxima (qualquer célula), média por célula.

    Para cada escola, a menor distância a outra federal em atividade; depois a
    média dessas mínimas por célula. O conjunto federal é pequeno, então o par a
    par (broadcast nested loop) é adequado.
    """
    left = schools.select(
        F.col("id_unidade").alias("l_id"),
        F.col("h3_cell").alias("l_cell"),
        F.col("geom_wgs84").alias("l_geom"),
    )
    right = schools.select(
        F.col("id_unidade").alias("r_id"),
        F.col("geom_wgs84").alias("r_geom"),
    )
    pairs = (
        left.join(F.broadcast(right), F.col("l_id") != F.col("r_id"), "inner")
        .withColumn("dist_km", geodesic_km("l_geom", "r_geom"))
    )
    per_school = pairs.groupBy("l_id", "l_cell").agg(
        F.min("dist_km").alias("nearest_km")
    )
    return per_school.groupBy("l_cell").agg(
        F.avg("nearest_km").alias("avg_dist_nearest_km")
    ).select(F.col("l_cell").alias("h3_cell"), "avg_dist_nearest_km")


def main():
    """Grade H3 nível 5 das escolas federais em atividade, com vizinhança."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = prepare_schools(spark.read.format("geoparquet").load(ORIGEM)).cache()

    grid = (
        cell_base_aggregates(schools)
        .join(cell_neighbor_distances(schools), on="h3_cell", how="left")
        .join(cell_neighbor_counts(schools), on="h3_cell", how="left")
        .join(cell_nearest_distances(schools), on="h3_cell", how="left")
        .withColumn(
            "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
        )
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
