from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
FEDERAL = 1
EM_ATIVIDADE = 1
METERS_PER_KM = 1000.0


def federal_active_schools(silver: DataFrame) -> DataFrame:
    """Escolas federais em atividade, com ponto WGS84 e célula H3 nível 5.

    O ``point`` é reconstruído em WGS84 (lon/lat) a partir das coordenadas
    originais: as distâncias geodésicas (``ST_DistanceSpheroid``) exigem 4326 e não
    se reprojeta o ponto 3857 de volta só para medir. A célula H3 sai desse ponto.
    """
    point = stc.ST_Point(F.col("longitude"), F.col("latitude"))
    cell = F.element_at(stf.ST_H3CellIDs(F.col("point"), H3_LEVEL, False), 1)
    return (
        silver.filter(
            (F.col("setor") == FEDERAL) & (F.col("situacao") == EM_ATIVIDADE)
        )
        .withColumn("point", point)
        .withColumn("h3_cell", cell)
        .select("id_unidade", "total_enrollment", "h3_cell", "point")
    )


def cell_aggregates(schools: DataFrame) -> DataFrame:
    """Contagem e matrículas por célula: ``n_schools``/``total_enrollment``/``avg``."""
    return schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("total_enrollment").cast("long").alias("total_enrollment"),
        F.round(F.avg("total_enrollment"), 2).alias("avg_enrollment"),
    )


def neighbor_counts(schools: DataFrame) -> DataFrame:
    """``n_federal_neighbors`` por célula = (escolas no disco kRing(1)) − 1.

    Cada escola é "coberta" pelas células do seu disco kRing(1) (própria + 6
    adjacentes). Como uma escola na célula X é vizinha do alvo C sse X está no
    disco de C (relação simétrica), basta explodir cada escola sobre o disco e
    contar por célula-alvo. Subtrai 1 para excluir a própria escola — o resultado
    é o número de vizinhas de qualquer escola da célula (igual para toda a célula).
    """
    ring = stf.ST_H3KRing(F.col("h3_cell"), 1, False)
    membership = schools.select("id_unidade", F.explode(ring).alias("target_cell"))
    return membership.groupBy("target_cell").agg(
        (F.count(F.lit(1)) - F.lit(1)).cast("long").alias("n_federal_neighbors")
    )


def neighbor_distance(schools: DataFrame) -> DataFrame:
    """``avg_dist_neighbors_km`` por célula (média das médias por escola).

    Para cada escola calcula a distância geodésica às vizinhas (escolas cujo
    disco kRing(1) contém a célula da escola) e tira a média; depois promedia
    essas médias na célula. Escolas isoladas não geram pares — a célula fica
    ``NULL`` quando nenhuma escola tem vizinha (ausência não é zero).
    """
    ring = stf.ST_H3KRing(F.col("h3_cell"), 1, False)
    origin = schools.select(
        F.col("id_unidade").alias("s_id"),
        F.col("h3_cell").alias("s_cell"),
        F.col("point").alias("s_point"),
        F.explode(ring).alias("ring_cell"),
    )
    target = schools.select(
        F.col("id_unidade").alias("t_id"),
        F.col("h3_cell").alias("t_cell"),
        F.col("point").alias("t_point"),
    )
    pairs = origin.join(
        target, F.col("ring_cell") == F.col("t_cell"), "inner"
    ).filter(F.col("s_id") != F.col("t_id"))

    per_school = pairs.withColumn(
        "dist_m", stf.ST_DistanceSpheroid(F.col("s_point"), F.col("t_point"))
    ).groupBy("s_cell", "s_id").agg(F.avg("dist_m").alias("school_avg_dist_m"))

    return (
        per_school.groupBy("s_cell")
        .agg(F.round(F.avg("school_avg_dist_m") / F.lit(METERS_PER_KM), 3).alias("avg_dist_neighbors_km"))
        .withColumnRenamed("s_cell", "h3_cell")
    )


def nearest_distance(schools: DataFrame) -> DataFrame:
    """``avg_dist_nearest_km`` por célula.

    Distância geodésica de cada escola à federal mais próxima (qualquer, sem
    restrição de célula), via produto cartesiano das federais em atividade, depois
    promediada por célula. O volume é pequeno (centenas de escolas), então o cross
    join é viável.
    """
    origin = schools.select(
        F.col("id_unidade").alias("s_id"),
        F.col("h3_cell").alias("s_cell"),
        F.col("point").alias("s_point"),
    )
    target = schools.select(
        F.col("id_unidade").alias("t_id"),
        F.col("point").alias("t_point"),
    )
    pairs = origin.crossJoin(target).filter(F.col("s_id") != F.col("t_id"))

    per_school = pairs.withColumn(
        "dist_m", stf.ST_DistanceSpheroid(F.col("s_point"), F.col("t_point"))
    ).groupBy("s_cell", "s_id").agg(F.min("dist_m").alias("nearest_m"))

    return (
        per_school.groupBy("s_cell")
        .agg(F.round(F.avg("nearest_m") / F.lit(METERS_PER_KM), 3).alias("avg_dist_nearest_km"))
        .withColumnRenamed("s_cell", "h3_cell")
    )


def main():
    """Grade H3 nível 5 das escolas federais em atividade (densidade + distâncias)."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("geoparquet").load(ORIGEM)
    schools = federal_active_schools(silver).cache()

    cells = cell_aggregates(schools)
    neighbors = neighbor_counts(schools)
    neigh_dist = neighbor_distance(schools)
    near_dist = nearest_distance(schools)

    cell_geom = F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
    grid = (
        cells.join(neighbors, cells["h3_cell"] == neighbors["target_cell"], "left")
        .drop("target_cell")
        .join(neigh_dist, "h3_cell", "left")
        .join(near_dist, "h3_cell", "left")
        .withColumn("geometry", cell_geom)
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
