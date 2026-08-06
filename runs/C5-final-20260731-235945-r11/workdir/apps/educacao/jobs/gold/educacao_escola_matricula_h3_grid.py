from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
FEDERAL_SETOR = "1"
ACTIVE_SITUACAO = "1"
# k=1 (célula + adjacentes imediatas); exact_ring=False inclui a própria célula.
KRING_K = 1
KRING_EXACT = False


def select_federal_active(df, level):
    """Filtra federais em atividade e anexa ponto WGS84 e célula H3 do nível.

    O ponto é reconstruído de latitude/longitude (WGS84) para servir tanto à
    indexação H3 quanto à distância geodésica — medidas sobre a coordenada
    original, sem reprojetar ida-e-volta.
    """
    is_federal = F.col("setor") == FEDERAL_SETOR
    is_active = F.col("situacao") == ACTIVE_SITUACAO
    fed = df.filter(is_federal & is_active)
    fed = dt.add_point_geometry(fed, "latitude", "longitude", "point")
    return dt.attach_h3_index(fed, "point", level, "h3_cell").select(
        "id_unidade", "enrollment", "h3_cell", "point"
    )


def summarize_cells(fed):
    """Contagem de escolas, total e média de matrículas por célula H3."""
    return fed.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("enrollment").cast("long").alias("total_enrollment"),
        F.round(F.avg("enrollment"), 2).alias("avg_enrollment"),
    )


def count_federal_neighbors(fed):
    """Nº de escolas vizinhas por célula (igual para todas as escolas da célula).

    Vizinhas de uma escola são as demais federais em atividade na própria célula
    ou nas adjacentes. Como o k-ring é o mesmo para toda a célula, esse total é
    ``(escolas na célula + adjacentes) - 1`` (a própria), idêntico para as escolas
    da célula.
    """
    cell_counts = fed.groupBy("h3_cell").agg(F.count(F.lit(1)).alias("cell_n"))
    ring = cell_counts.select(
        F.col("h3_cell").alias("center"),
        F.explode(stf.ST_H3KRing(F.col("h3_cell"), KRING_K, KRING_EXACT)).alias("cell"),
    )
    neighbor_counts = cell_counts.select(
        F.col("h3_cell").alias("cell"), F.col("cell_n").alias("neighbor_n")
    )
    pooled = ring.join(neighbor_counts, on="cell", how="left")
    return (
        pooled.groupBy("center")
        .agg(F.sum(F.coalesce("neighbor_n", F.lit(0))).alias("pool_n"))
        .select(
            F.col("center").alias("h3_cell"),
            (F.col("pool_n") - F.lit(1)).cast("long").alias("n_federal_neighbors"),
        )
    )


def average_pairwise_distances(fed):
    """Distâncias médias por célula: às vizinhas e à federal mais próxima (qualquer).

    Como as federais em atividade são poucas (~centenas), um cross join (N²) é
    barato. Para cada escola calcula-se a média até as vizinhas (célula +
    adjacentes) e o mínimo até qualquer outra federal; ambos são então
    promediados sobre as escolas da célula. Distância geodésica (km) sobre o ponto
    WGS84 original.
    """
    schools = fed.select(
        "id_unidade",
        "h3_cell",
        "point",
        stf.ST_H3KRing(F.col("h3_cell"), KRING_K, KRING_EXACT).alias("ring"),
    )
    left = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("point").alias("a_point"),
        F.col("ring").alias("a_ring"),
    )
    right = schools.select(
        F.col("id_unidade").alias("b_id"),
        F.col("h3_cell").alias("b_cell"),
        F.col("point").alias("b_point"),
    )
    pairs = (
        left.crossJoin(F.broadcast(right))
        .filter(F.col("a_id") != F.col("b_id"))
        .withColumn(
            "dist_km",
            stf.ST_DistanceSpheroid(F.col("a_point"), F.col("b_point")) / F.lit(1000.0),
        )
    )

    is_neighbor = F.array_contains(F.col("a_ring"), F.col("b_cell"))
    per_school = pairs.groupBy("a_id", "a_cell").agg(
        F.min("dist_km").alias("nearest_km"),
        F.avg(F.when(is_neighbor, F.col("dist_km"))).alias("neighbor_mean_km"),
    )
    return (
        per_school.groupBy("a_cell")
        .agg(
            F.round(F.avg("neighbor_mean_km"), 3).alias("avg_dist_neighbors_km"),
            F.round(F.avg("nearest_km"), 3).alias("avg_dist_nearest_km"),
        )
        .select(
            F.col("a_cell").alias("h3_cell"),
            "avg_dist_neighbors_km",
            "avg_dist_nearest_km",
        )
    )


def add_cell_geometry(df):
    """Reconstrói o polígono da célula H3 (4326) e projeta para 3857."""
    with_geom = df.withColumn(
        "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
    )
    return dt.to_web_mercator(with_geom)


def main():
    """Grade H3 nível 5 das escolas federais em atividade, com métricas de vizinhança."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    geo = spark.read.format("geoparquet").load(ORIGEM)
    fed = select_federal_active(geo, H3_LEVEL).cache()

    grid = (
        summarize_cells(fed)
        .join(count_federal_neighbors(fed), on="h3_cell", how="left")
        .join(average_pairwise_distances(fed), on="h3_cell", how="left")
        .transform(add_cell_geometry)
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
