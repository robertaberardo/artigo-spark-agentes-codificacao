from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
# k=1 (com exact_ring=False) devolve a célula e as adjacentes imediatas.
KRING_K = 1
KRING_EXACT = False
METERS_PER_KM = 1000.0


def geodesic_km(point_a, point_b):
    """Distância geodésica (km) entre dois pontos WGS84, via ``ST_DistanceSpheroid``.

    Mede sobre o elipsoide WGS84, nas coordenadas originais em graus — nunca sobre
    a projeção métrica, que distorce os metros nas latitudes do Brasil.
    """
    return stf.ST_DistanceSpheroid(point_a, point_b) / F.lit(METERS_PER_KM)


def prepare_base(geo: DataFrame) -> DataFrame:
    """Reconstrói o ponto WGS84 original e anexa a célula H3 nível 5.

    O ponto é remontado de lat/long (não da geometria 3857 persistida) para que
    as distâncias sejam medidas nas coordenadas originais. A célula H3 é calculada
    em EPSG:4326, como o H3 exige.
    """
    return (
        geo.select("id_unidade", "total_enrollment", "latitude", "longitude")
        .withColumn("point", stc.ST_Point(F.col("longitude"), F.col("latitude")))
        .withColumn(
            "h3_cell", F.element_at(stf.ST_H3CellIDs(F.col("point"), H3_LEVEL, False), 1)
        )
    )


def cell_enrollment_stats(base: DataFrame) -> DataFrame:
    """Contagem de escolas, total e média de matrículas por célula."""
    return base.groupBy("h3_cell").agg(
        F.count(F.lit(1)).alias("n_schools"),
        F.sum("total_enrollment").alias("total_enrollment"),
        F.round(F.avg("total_enrollment"), 1).alias("avg_enrollment"),
    )


def neighbor_distances(base: DataFrame) -> DataFrame:
    """Vizinhança por célula: quantidade de vizinhas e distância média a elas.

    São vizinhas as outras escolas federais em atividade na própria célula ou nas
    adjacentes (disco H3 de raio 1). Explode-se o disco de cada escola em células
    candidatas e casa-se com as escolas que caem nelas; o par com a própria escola
    é descartado. A contagem de vizinhas é idêntica para toda a célula (todas as
    escolas da célula compartilham o mesmo disco), então basta ``max`` por célula.
    """
    origin = base.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("point").alias("a_point"),
    ).withColumn(
        "ring_cell", F.explode(stf.ST_H3KRing(F.col("a_cell"), KRING_K, KRING_EXACT))
    )
    candidate = base.select(
        F.col("id_unidade").alias("b_id"),
        F.col("h3_cell").alias("b_cell"),
        F.col("point").alias("b_point"),
    )

    pairs = origin.join(
        candidate, origin["ring_cell"] == candidate["b_cell"], "inner"
    ).filter(F.col("a_id") != F.col("b_id"))

    per_school = pairs.groupBy("a_id", "a_cell").agg(
        F.count(F.lit(1)).alias("n_neighbors"),
        F.avg(geodesic_km(F.col("a_point"), F.col("b_point"))).alias("dist_neighbors_km"),
    )

    return per_school.groupBy("a_cell").agg(
        F.max("n_neighbors").alias("n_federal_neighbors"),
        F.round(F.avg("dist_neighbors_km"), 3).alias("avg_dist_neighbors_km"),
    ).select(
        F.col("a_cell").alias("h3_cell"),
        "n_federal_neighbors",
        "avg_dist_neighbors_km",
    )


def nearest_federal_distances(base: DataFrame) -> DataFrame:
    """Distância média, por célula, de cada escola à federal mais próxima.

    Sem restrição de célula: cruza todas as escolas federais em atividade entre si
    (o universo já é só federais ativas), toma a menor distância de cada uma e
    tira a média por célula. O conjunto é pequeno (centenas), então o produto
    cartesiano é barato.
    """
    origin = base.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("point").alias("a_point"),
    )
    other = base.select(
        F.col("id_unidade").alias("o_id"),
        F.col("point").alias("o_point"),
    )

    nearest = (
        origin.crossJoin(other)
        .filter(F.col("a_id") != F.col("o_id"))
        .groupBy("a_id", "a_cell")
        .agg(F.min(geodesic_km(F.col("a_point"), F.col("o_point"))).alias("dist_nearest_km"))
    )

    return nearest.groupBy("a_cell").agg(
        F.round(F.avg("dist_nearest_km"), 3).alias("avg_dist_nearest_km")
    ).select(F.col("a_cell").alias("h3_cell"), "avg_dist_nearest_km")


def add_cell_geometry(df: DataFrame) -> DataFrame:
    """Anexa o polígono da célula H3 em EPSG:3857 (convenção de saída do lake)."""
    cell_geom = F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
    return df.withColumn(
        "geometry", stf.ST_Transform(cell_geom, F.lit("EPSG:4326"), F.lit("EPSG:3857"))
    )


def main():
    """Grade H3 nível 5 das escolas federais em atividade, com matrículas e vizinhança."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    geo = spark.read.format("geoparquet").load(ORIGEM)
    base = prepare_base(geo).cache()

    stats = cell_enrollment_stats(base)
    neighbors = neighbor_distances(base)
    nearest = nearest_federal_distances(base)

    grid = (
        stats.join(neighbors, on="h3_cell", how="left")
        .join(nearest, on="h3_cell", how="left")
        # Célula sem vizinhas (escola isolada na própria célula e adjacentes): a
        # contagem é 0 de fato; a distância média fica NULL (indefinida).
        .withColumn("n_federal_neighbors", F.coalesce(F.col("n_federal_neighbors"), F.lit(0)))
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
