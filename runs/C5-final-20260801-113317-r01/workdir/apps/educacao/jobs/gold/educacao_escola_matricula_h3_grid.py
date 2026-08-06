from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_functions as stf

from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
K_RING = 1  # disco de raio 1: a própria célula + as imediatamente adjacentes
SETOR_FEDERAL = 1
SITUACAO_ATIVA = 1
METERS_PER_KM = 1000.0


def build_federal_schools(geo: DataFrame) -> DataFrame:
    """Filtra escolas federais em atividade e anexa a célula H3 nível 5.

    Universo de análise: só ``setor`` federal e ``situacao`` em atividade. O H3 é
    calculado sobre o ponto WGS84 original (``ST_H3CellIDs`` opera em graus).
    """
    is_federal = F.col("setor") == SETOR_FEDERAL
    is_active = F.col("situacao") == SITUACAO_ATIVA
    return (
        geo.filter(is_federal & is_active)
        .withColumn(
            "h3_cell",
            F.element_at(stf.ST_H3CellIDs(F.col("geometry"), H3_LEVEL, False), 1),
        )
        .select("id_unidade", "enrollment", "geometry", "h3_cell")
    )


def cell_base_metrics(schools: DataFrame) -> DataFrame:
    """Contagem de escolas, total e média de matrículas por célula."""
    return schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("enrollment").cast("long").alias("total_enrollment"),
        F.avg("enrollment").alias("avg_enrollment"),
    )


def nearest_federal_per_cell(schools: DataFrame) -> DataFrame:
    """Distância média (km) à federal mais próxima, sem restrição de célula.

    Para cada escola mede a distância geodésica à federal em atividade mais
    próxima (qualquer, em todo o país) e devolve a média dessas distâncias por
    célula.
    """
    left = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("geometry").alias("a_geom"),
    )
    right = schools.select(
        F.col("id_unidade").alias("b_id"),
        F.col("geometry").alias("b_geom"),
    )
    pairs = left.crossJoin(F.broadcast(right)).filter(F.col("a_id") != F.col("b_id"))
    dist_km = stf.ST_DistanceSpheroid(F.col("a_geom"), F.col("b_geom")) / METERS_PER_KM

    nearest = pairs.withColumn("dist_km", dist_km).groupBy("a_id", "a_cell").agg(
        F.min("dist_km").alias("nearest_km")
    )
    return nearest.groupBy("a_cell").agg(
        F.avg("nearest_km").alias("avg_dist_nearest_km")
    ).select(F.col("a_cell").alias("h3_cell"), F.col("avg_dist_nearest_km"))


def neighbor_metrics_per_cell(schools: DataFrame) -> DataFrame:
    """Vizinhas no disco H3: contagem por célula e distância média às vizinhas.

    Vizinhas de uma escola são as outras federais em atividade na própria célula
    ou nas adjacentes (disco ``ST_H3KRing`` de raio 1). A contagem é a mesma para
    toda a célula (todas partilham o disco); ``avg_dist_neighbors_km`` é a média,
    na célula, da distância geodésica média de cada escola às suas vizinhas.
    """
    left = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("geometry").alias("a_geom"),
    )
    disk = left.withColumn(
        "disk_cell", F.explode(stf.ST_H3KRing(F.col("a_cell"), K_RING, False))
    )
    right = schools.select(
        F.col("id_unidade").alias("b_id"),
        F.col("h3_cell").alias("b_cell"),
        F.col("geometry").alias("b_geom"),
    )
    same_cell = F.col("disk_cell") == F.col("b_cell")
    not_self = F.col("a_id") != F.col("b_id")
    dist_km = stf.ST_DistanceSpheroid(F.col("a_geom"), F.col("b_geom")) / METERS_PER_KM

    pairs = (
        disk.join(F.broadcast(right), same_cell, "inner")
        .filter(not_self)
        .withColumn("dist_km", dist_km)
    )
    per_school = pairs.groupBy("a_id", "a_cell").agg(
        F.avg("dist_km").alias("mean_nb_km"),
        F.count(F.lit(1)).cast("long").alias("n_nb"),
    )

    # Escolas sem vizinhas somem no inner join; reincorpora-as com n_nb=0 para não
    # sumir a célula nem enviesar a contagem.
    all_schools = schools.select(
        F.col("id_unidade").alias("a_id"), F.col("h3_cell").alias("a_cell")
    )
    complete = all_schools.join(per_school, ["a_id", "a_cell"], "left").select(
        F.col("a_cell"),
        F.col("mean_nb_km"),
        F.coalesce(F.col("n_nb"), F.lit(0)).alias("n_nb"),
    )
    return complete.groupBy("a_cell").agg(
        F.avg("mean_nb_km").alias("avg_dist_neighbors_km"),
        F.max("n_nb").alias("n_federal_neighbors"),
    ).select(
        F.col("a_cell").alias("h3_cell"),
        F.col("avg_dist_neighbors_km"),
        F.col("n_federal_neighbors"),
    )


def main():
    """Grade H3 nível 5 das escolas federais em atividade com matrículas e vizinhança."""
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    geo = spark.read.format("geoparquet").load(ORIGEM)
    schools = build_federal_schools(geo).cache()

    base = cell_base_metrics(schools)
    neighbors = neighbor_metrics_per_cell(schools)
    nearest = nearest_federal_per_cell(schools)

    grid = base.join(neighbors, "h3_cell", "left").join(nearest, "h3_cell", "left")

    result = grid.withColumn(
        "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
    ).select(
        F.col("h3_cell"),
        F.col("n_schools"),
        F.col("total_enrollment"),
        F.round(F.col("avg_enrollment"), 2).alias("avg_enrollment"),
        F.col("n_federal_neighbors").cast("long").alias("n_federal_neighbors"),
        F.round(F.col("avg_dist_neighbors_km"), 3).alias("avg_dist_neighbors_km"),
        F.round(F.col("avg_dist_nearest_km"), 3).alias("avg_dist_nearest_km"),
        F.col("geometry"),
    )

    result.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
