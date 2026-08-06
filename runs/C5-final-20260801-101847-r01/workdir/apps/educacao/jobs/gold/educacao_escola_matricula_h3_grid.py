from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
KM = 1000.0  # ST_DistanceSpheroid devolve metros; dividimos para km


def prepare_schools(geo: DataFrame) -> DataFrame:
    """Ponto WGS84 (para distância geodésica) e célula H3 nível 5 de cada escola.

    Reconstrói o ponto a partir da lat/long originais — não da geometria 3857 do
    silver — para medir distância sobre as coordenadas originais (sem round-trip).
    """
    return (
        geo.transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude", "point"))
        .transform(lambda d: dt.attach_h3_index(d, "point", H3_LEVEL, "h3_cell"))
        .select("id_unidade", "h3_cell", "total_enrollment", "point")
    )


def cell_basics(schools: DataFrame) -> DataFrame:
    """Contagem de escolas, total e média de matrículas por célula."""
    return schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("total_enrollment").cast("long").alias("total_enrollment"),
        F.round(F.avg("total_enrollment"), 2).alias("avg_enrollment"),
    )


def count_neighbor_schools(schools: DataFrame) -> DataFrame:
    """Escolas federais na vizinhança da célula (própria + adjacentes).

    Valor de nível de célula (igual para toda a célula): soma as escolas de todas
    as células do k-ring 1 da célula central. Como o disco inclui a própria
    célula, as escolas dela também entram na contagem.
    """
    per_cell = schools.groupBy("h3_cell").agg(F.count(F.lit(1)).alias("cell_count"))
    members = (
        per_cell.select("h3_cell")
        .transform(lambda d: dt.add_h3_kring(d, "h3_cell", 1, "h3_kring"))
        .select(
            F.col("h3_cell").alias("center_cell"),
            F.explode("h3_kring").alias("member_cell"),
        )
    )
    counts = per_cell.select(
        F.col("h3_cell").alias("member_cell"), "cell_count"
    )
    return (
        members.join(counts, "member_cell", "inner")
        .groupBy("center_cell")
        .agg(F.sum("cell_count").cast("long").alias("n_federal_neighbors"))
        .select(F.col("center_cell").alias("h3_cell"), "n_federal_neighbors")
    )


def _pairwise_distance_km(left: DataFrame, right: DataFrame, join_on) -> DataFrame:
    """Pares de escolas distintas com a distância geodésica (km) entre elas.

    ``join_on`` define o critério de emparelhamento (célula da vizinhança ou
    produto cartesiano). A distância usa ``ST_DistanceSpheroid`` (WGS84).
    """
    pairs = left.join(right, join_on) if join_on is not None else left.crossJoin(right)
    return pairs.filter(F.col("id_i") != F.col("id_j")).withColumn(
        "dist_km", stf.ST_DistanceSpheroid(F.col("point_i"), F.col("point_j")) / KM
    )


def avg_neighbor_distance(schools: DataFrame) -> DataFrame:
    """Média por célula da distância média de cada escola às suas vizinhas.

    Vizinhas = outras escolas cuja célula está no k-ring 1 da escola. Primeiro a
    média por escola, depois a média entre as escolas da célula.
    """
    i_side = (
        schools.transform(lambda d: dt.add_h3_kring(d, "h3_cell", 1, "h3_kring"))
        .select(
            F.col("id_unidade").alias("id_i"),
            F.col("h3_cell").alias("cell_i"),
            F.col("point").alias("point_i"),
            F.explode("h3_kring").alias("ncell"),
        )
    )
    j_side = schools.select(
        F.col("id_unidade").alias("id_j"),
        F.col("h3_cell").alias("ncell"),
        F.col("point").alias("point_j"),
    )
    per_school = _pairwise_distance_km(i_side, j_side, "ncell").groupBy(
        "id_i", "cell_i"
    ).agg(F.avg("dist_km").alias("school_avg_km"))
    return per_school.groupBy("cell_i").agg(
        F.round(F.avg("school_avg_km"), 2).alias("avg_dist_neighbors_km")
    ).select(F.col("cell_i").alias("h3_cell"), "avg_dist_neighbors_km")


def avg_nearest_distance(schools: DataFrame) -> DataFrame:
    """Média por célula da distância de cada escola à federal mais próxima.

    A federal mais próxima é a de menor distância entre todas (sem restrição de
    célula): primeiro o mínimo por escola, depois a média entre as da célula.
    """
    i_side = schools.select(
        F.col("id_unidade").alias("id_i"),
        F.col("h3_cell").alias("cell_i"),
        F.col("point").alias("point_i"),
    )
    j_side = schools.select(
        F.col("id_unidade").alias("id_j"), F.col("point").alias("point_j")
    )
    per_school = _pairwise_distance_km(i_side, j_side, None).groupBy(
        "id_i", "cell_i"
    ).agg(F.min("dist_km").alias("school_nearest_km"))
    return per_school.groupBy("cell_i").agg(
        F.round(F.avg("school_nearest_km"), 2).alias("avg_dist_nearest_km")
    ).select(F.col("cell_i").alias("h3_cell"), "avg_dist_nearest_km")


def add_cell_geometry(df: DataFrame) -> DataFrame:
    """Anexa o polígono da célula H3 em EPSG:3857 (convenção de saída do lake)."""
    cell_geom = F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
    return df.withColumn(
        "geometry", stf.ST_Transform(cell_geom, F.lit("EPSG:4326"), F.lit("EPSG:3857"))
    )


def main():
    """Grade H3 nível 5 das escolas federais em atividade, com vizinhança e distâncias."""
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = prepare_schools(spark.read.format("geoparquet").load(ORIGEM)).cache()

    grid = (
        cell_basics(schools)
        .join(count_neighbor_schools(schools), "h3_cell", "left")
        .join(avg_neighbor_distance(schools), "h3_cell", "left")
        .join(avg_nearest_distance(schools), "h3_cell", "left")
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
