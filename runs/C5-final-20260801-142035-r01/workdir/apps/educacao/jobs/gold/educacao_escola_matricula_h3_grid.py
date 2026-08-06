from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
SETOR_FEDERAL = "1"
SITUACAO_ATIVA = "1"
# Vizinhança: mesma célula (distância de grade H3 = 0) ou adjacente (= 1).
ADJACENT_CELL_DISTANCE = 1
METERS_PER_KM = 1000.0


def select_active_federal_schools(df: DataFrame) -> DataFrame:
    """Filtra escolas federais em atividade e anexa ponto WGS84 e célula H3 nível 5.

    A distância geodésica e o H3 operam em WGS84, por isso o ponto é reconstruído de
    latitude/longitude originais (não do ``geometry`` em 3857 da silver).
    """
    is_federal = F.col("setor") == SETOR_FEDERAL
    is_active = F.col("situacao") == SITUACAO_ATIVA
    point_wgs = stc.ST_Point(F.col("longitude"), F.col("latitude"))
    return (
        df.filter(is_federal & is_active)
        .withColumn("point_wgs", point_wgs)
        .withColumn(
            "h3_cell",
            F.element_at(stf.ST_H3CellIDs(F.col("point_wgs"), H3_LEVEL, False), 1),
        )
        .select("id_unidade", "total_enrollment", "h3_cell", "point_wgs")
    )


def pairwise_school_distances(schools: DataFrame) -> DataFrame:
    """Distância geodésica e vizinhança entre cada par ordenado de escolas federais.

    O conjunto de federais é pequeno, então o produto cartesiano (com broadcast) é
    barato. São vizinhas as escolas na mesma célula ou em célula imediatamente
    adjacente (distância de grade H3 <= 1).
    """
    left = schools.alias("a")
    right = schools.alias("b")
    dist_m = stf.ST_DistanceSpheroid(F.col("a.point_wgs"), F.col("b.point_wgs"))
    cell_dist = stf.ST_H3CellDistance(F.col("a.h3_cell"), F.col("b.h3_cell"))
    return left.join(
        F.broadcast(right), F.col("a.id_unidade") != F.col("b.id_unidade"), "inner"
    ).select(
        F.col("a.id_unidade").alias("school_id"),
        F.col("a.h3_cell").alias("h3_cell"),
        F.col("a.total_enrollment").alias("total_enrollment"),
        dist_m.alias("dist_m"),
        (cell_dist <= ADJACENT_CELL_DISTANCE).alias("is_neighbor"),
    )


def per_school_metrics(pairs: DataFrame) -> DataFrame:
    """Por escola: distância à federal mais próxima, nº de vizinhas e média até elas.

    ``F.avg`` sobre a distância só das vizinhas ignora as demais (viram ``NULL``);
    escola sem vizinha fica com ``avg_neighbor_dist_m`` nulo e ``n_neighbors`` = 0.
    """
    neighbor_dist = F.when(F.col("is_neighbor"), F.col("dist_m"))
    neighbor_flag = F.when(F.col("is_neighbor"), F.lit(1)).otherwise(F.lit(0))
    return pairs.groupBy("school_id", "h3_cell", "total_enrollment").agg(
        F.min("dist_m").alias("nearest_dist_m"),
        F.sum(neighbor_flag).alias("n_neighbors"),
        F.avg(neighbor_dist).alias("avg_neighbor_dist_m"),
    )


def aggregate_cells(schools: DataFrame, metrics: DataFrame) -> DataFrame:
    """Agrega por célula H3 e reconstrói o polígono da célula em EPSG:3857.

    Contagem e matrículas vêm das escolas (denominador correto, mesmo se alguma
    escola não aparecer nos pares); as métricas de distância vêm por escola. O nº de
    vizinhas é idêntico para todas as escolas de uma célula (compartilham a mesma
    vizinhança de células), então ``F.max`` apenas materializa esse valor constante.
    """
    counts = schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("total_enrollment").cast("long").alias("total_enrollment"),
        F.avg("total_enrollment").alias("avg_enrollment"),
    )
    distances = metrics.groupBy("h3_cell").agg(
        F.max("n_neighbors").cast("long").alias("n_federal_neighbors"),
        F.avg("avg_neighbor_dist_m").alias("avg_dist_neighbors_m"),
        F.avg("nearest_dist_m").alias("avg_dist_nearest_m"),
    )
    grid = counts.join(distances, on="h3_cell", how="left")
    return grid.withColumn(
        "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
    ).transform(dt.to_web_mercator)


def main():
    """Grade H3 nível 5 de escolas federais em atividade, com matrículas e distâncias."""
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = select_active_federal_schools(
        spark.read.format("geoparquet").load(ORIGEM)
    ).cache()

    metrics = per_school_metrics(pairwise_school_distances(schools))
    grid = aggregate_cells(schools, metrics)

    result = grid.select(
        F.col("h3_cell"),
        F.col("n_schools"),
        F.col("total_enrollment"),
        F.round("avg_enrollment", 2).alias("avg_enrollment"),
        F.col("n_federal_neighbors"),
        F.round(F.col("avg_dist_neighbors_m") / METERS_PER_KM, 3).alias(
            "avg_dist_neighbors_km"
        ),
        F.round(F.col("avg_dist_nearest_m") / METERS_PER_KM, 3).alias(
            "avg_dist_nearest_km"
        ),
        F.col("geometry"),
    )

    result.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
