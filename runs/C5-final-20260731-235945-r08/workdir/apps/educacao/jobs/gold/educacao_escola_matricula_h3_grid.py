from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
FEDERAL = 1  # setor administrativo federal
ACTIVE = 1  # situação "em atividade"
METERS_PER_KM = 1000.0


def prepare_federal_active_schools(silver: DataFrame) -> DataFrame:
    """Filtra federais em atividade e anexa ponto WGS84 + célula H3 nível 5.

    O ponto é reconstruído a partir das lat/long originais (WGS84) para que as
    distâncias sejam geodésicas sobre as coordenadas originais, e o índice H3 é
    calculado sobre esse mesmo ponto (o H3 opera em graus).
    """
    is_federal = F.col("setor") == FEDERAL
    is_active = F.col("situacao") == ACTIVE
    return (
        silver.filter(is_federal & is_active)
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude", "geom_wgs"))
        .transform(lambda d: dt.attach_h3_index(d, "geom_wgs", H3_LEVEL, "h3_cell"))
        .select("id_unidade", "h3_cell", "geom_wgs", "total_enrollment")
    )


def aggregate_cell_base(schools: DataFrame) -> DataFrame:
    """Contagem de escolas, total e média de matrículas por célula.

    ``avg`` e ``sum`` ignoram nulos (matrícula ausente não é zero), então a média
    considera apenas escolas com matrícula informada.
    """
    return schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("total_enrollment").cast("long").alias("total_enrollment"),
        F.avg("total_enrollment").alias("avg_enrollment"),
    )


def count_federal_neighbors_per_cell(schools: DataFrame) -> DataFrame:
    """Nº de federais em atividade na célula + adjacentes (mesmo p/ toda a célula).

    Soma, para cada célula-alvo, as escolas contidas nela e em cada célula do seu
    anel-1 (``ST_H3KRing`` com ``k=1``), formando o tamanho do pool de vizinhança.
    """
    cell_counts = schools.groupBy("h3_cell").agg(F.count(F.lit(1)).alias("n_in_cell"))
    target = cell_counts.select(
        F.col("h3_cell").alias("target_cell"),
        F.explode(stf.ST_H3KRing(F.col("h3_cell"), 1, False)).alias("member_cell"),
    )
    return (
        target.join(
            cell_counts, target["member_cell"] == cell_counts["h3_cell"], how="left"
        )
        .groupBy("target_cell")
        .agg(F.sum("n_in_cell").cast("long").alias("n_federal_neighbors"))
        .select(F.col("target_cell").alias("h3_cell"), "n_federal_neighbors")
    )


def average_neighbor_distance_per_cell(schools: DataFrame) -> DataFrame:
    """Distância geodésica média de cada escola às suas vizinhas, média por célula.

    Vizinhas = outras federais em atividade na própria célula ou nas adjacentes
    (anel-1). Mede-se a distância por escola e depois a média entre as escolas da
    célula.
    """
    left = schools.select(
        F.col("id_unidade").alias("id_a"),
        F.col("h3_cell").alias("cell_a"),
        F.col("geom_wgs").alias("geom_a"),
    ).withColumn("ring_cell", F.explode(stf.ST_H3KRing(F.col("cell_a"), 1, False)))
    right = schools.select(
        F.col("id_unidade").alias("id_b"),
        F.col("h3_cell").alias("cell_b"),
        F.col("geom_wgs").alias("geom_b"),
    )
    pairs = left.join(
        right, F.col("ring_cell") == F.col("cell_b"), how="inner"
    ).filter(F.col("id_a") != F.col("id_b"))
    per_school = (
        pairs.withColumn(
            "dist_m", stf.ST_DistanceSpheroid(F.col("geom_a"), F.col("geom_b"))
        )
        .groupBy("id_a", "cell_a")
        .agg(F.avg("dist_m").alias("neighbor_mean_m"))
    )
    return (
        per_school.groupBy("cell_a")
        .agg(F.avg("neighbor_mean_m").alias("avg_dist_neighbors_m"))
        .select(F.col("cell_a").alias("h3_cell"), "avg_dist_neighbors_m")
    )


def average_nearest_distance_per_cell(schools: DataFrame) -> DataFrame:
    """Distância à federal mais próxima (qualquer célula), média por célula.

    Para cada escola, a menor distância geodésica a qualquer outra federal em
    atividade (sem restrição de célula); depois a média entre as escolas da célula.
    """
    left = schools.select(
        F.col("id_unidade").alias("id_a"),
        F.col("h3_cell").alias("cell_a"),
        F.col("geom_wgs").alias("geom_a"),
    )
    right = schools.select(
        F.col("id_unidade").alias("id_b"), F.col("geom_wgs").alias("geom_b")
    )
    pairs = left.crossJoin(right).filter(F.col("id_a") != F.col("id_b"))
    per_school = (
        pairs.withColumn(
            "dist_m", stf.ST_DistanceSpheroid(F.col("geom_a"), F.col("geom_b"))
        )
        .groupBy("id_a", "cell_a")
        .agg(F.min("dist_m").alias("nearest_m"))
    )
    return (
        per_school.groupBy("cell_a")
        .agg(F.avg("nearest_m").alias("avg_dist_nearest_m"))
        .select(F.col("cell_a").alias("h3_cell"), "avg_dist_nearest_m")
    )


def build_grid(schools: DataFrame) -> DataFrame:
    """Junta as métricas por célula e monta o schema final com a geometria H3."""
    base = aggregate_cell_base(schools)
    neighbors_count = count_federal_neighbors_per_cell(schools)
    neighbors_dist = average_neighbor_distance_per_cell(schools)
    nearest_dist = average_nearest_distance_per_cell(schools)

    return (
        base.join(neighbors_count, on="h3_cell", how="left")
        .join(neighbors_dist, on="h3_cell", how="left")
        .join(nearest_dist, on="h3_cell", how="left")
        .select(
            F.col("h3_cell"),
            F.col("n_schools"),
            F.col("total_enrollment"),
            F.round(F.col("avg_enrollment"), 2).alias("avg_enrollment"),
            F.col("n_federal_neighbors"),
            F.round(F.col("avg_dist_neighbors_m") / METERS_PER_KM, 3).alias(
                "avg_dist_neighbors_km"
            ),
            F.round(F.col("avg_dist_nearest_m") / METERS_PER_KM, 3).alias(
                "avg_dist_nearest_km"
            ),
            F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1).alias("geometry"),
        )
    )


def main():
    """Grade H3 (nível 5) de escolas federais em atividade + matrículas + vizinhança."""
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("geoparquet").load(ORIGEM)
    schools = prepare_federal_active_schools(silver).cache()

    grid = build_grid(schools)

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
