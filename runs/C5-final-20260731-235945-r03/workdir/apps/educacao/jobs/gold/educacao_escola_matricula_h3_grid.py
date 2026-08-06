from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
NEIGHBOR_K = 1  # vizinhas = própria célula (k=0) + células adjacentes (anel k=1)
M_PER_KM = 1000.0


def build_schools(geo):
    """Reconstrói o ponto WGS84 e indexa cada escola na célula H3 nível 5.

    A distância geodésica e o índice H3 são calculados sobre a coordenada
    original (WGS84), não sobre a geometria projetada em 3857 do silver.
    """
    return (
        geo.withColumn("point_wgs", stc.ST_Point(F.col("longitude"), F.col("latitude")))
        .transform(lambda d: dt.attach_h3_index(d, "point_wgs", H3_LEVEL, "h3_cell"))
        .select("id_unidade", "enrollment", "h3_cell", "point_wgs")
    )


def neighbor_stats(schools):
    """Por escola: nº de vizinhas e distância média (m) até elas.

    Vizinhas = outras escolas na própria célula ou nas adjacentes (disco H3
    ``k=1``). O disco é expandido por ``ST_H3KRing`` e cruzado com as escolas
    situadas nas células resultantes.
    """
    disk = schools.select(
        F.col("id_unidade").alias("s_id"),
        F.col("point_wgs").alias("s_point"),
        F.explode(
            stf.ST_H3KRing(F.col("h3_cell"), F.lit(NEIGHBOR_K), F.lit(False))
        ).alias("disk_cell"),
    )
    is_neighbor = (F.col("disk_cell") == F.col("t.h3_cell")) & (
        F.col("s_id") != F.col("t.id_unidade")
    )
    pairs = disk.join(schools.alias("t"), is_neighbor, "inner").select(
        "s_id",
        stf.ST_DistanceSpheroid(F.col("s_point"), F.col("t.point_wgs")).alias("dist_m"),
    )
    return pairs.groupBy("s_id").agg(
        F.count(F.lit(1)).cast("long").alias("n_neighbors"),
        F.avg("dist_m").alias("avg_dist_neighbors_m"),
    )


def nearest_stats(schools):
    """Por escola: distância (m) até a federal mais próxima, sem restrição de célula."""
    left = schools.select(
        F.col("id_unidade").alias("s_id"), F.col("point_wgs").alias("s_point")
    )
    right = schools.select(
        F.col("id_unidade").alias("t_id"), F.col("point_wgs").alias("t_point")
    )
    pairs = (
        left.crossJoin(F.broadcast(right))
        .filter(F.col("s_id") != F.col("t_id"))
        .select(
            "s_id",
            stf.ST_DistanceSpheroid(F.col("s_point"), F.col("t_point")).alias("dist_m"),
        )
    )
    return pairs.groupBy("s_id").agg(F.min("dist_m").alias("nearest_dist_m"))


def enrich_schools(schools):
    """Anexa a cada escola as estatísticas de vizinhança e de vizinha mais próxima."""
    neighbors = neighbor_stats(schools).withColumnRenamed("s_id", "id_unidade")
    nearest = nearest_stats(schools).withColumnRenamed("s_id", "id_unidade")
    with_neighbors = dt.join_no_fanout(
        schools.select("id_unidade", "h3_cell", "enrollment"),
        neighbors,
        on="id_unidade",
        how="left",
    )
    return dt.join_no_fanout(with_neighbors, nearest, on="id_unidade", how="left")


def main():
    """Gold: agregação por célula H3 nível 5 das escolas federais em atividade."""
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = build_schools(spark.read.format("geoparquet").load(ORIGEM)).cache()
    per_school = enrich_schools(schools)

    agg_exprs = [
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("enrollment").cast("long").alias("total_enrollment"),
        F.avg("enrollment").alias("avg_enrollment"),
        F.coalesce(F.max("n_neighbors"), F.lit(0)).cast("long").alias("n_federal_neighbors"),
        (F.avg("avg_dist_neighbors_m") / F.lit(M_PER_KM)).alias("avg_dist_neighbors_km"),
        (F.avg("nearest_dist_m") / F.lit(M_PER_KM)).alias("avg_dist_nearest_km"),
    ]
    grid = dt.aggregate_by_h3(per_school, "h3_cell", agg_exprs).select(
        "h3_cell",
        "n_schools",
        "total_enrollment",
        "avg_enrollment",
        "n_federal_neighbors",
        "avg_dist_neighbors_km",
        "avg_dist_nearest_km",
        "geometry",
    )

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
