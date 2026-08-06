from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
FEDERAL = "1"  # setor administrativo federal
EM_ATIVIDADE = "1"  # situação de funcionamento em atividade


def federal_active_schools(silver: DataFrame) -> DataFrame:
    """Escolas federais em atividade, com ponto WGS84 e célula H3 nível 5.

    Mantém o ponto em coordenadas originais (EPSG:4326) para medir distâncias
    geodesicamente antes de qualquer projeção, e indexa cada escola na célula H3
    (o H3 opera em graus). ``ST_Point`` espera ``(x=longitude, y=latitude)``.
    """
    point = stc.ST_Point(F.col("longitude"), F.col("latitude"))
    return (
        silver.filter(
            (F.col("setor") == FEDERAL) & (F.col("situacao") == EM_ATIVIDADE)
        )
        .withColumn("point", point)
        .withColumn(
            "h3_cell",
            F.element_at(stf.ST_H3CellIDs(F.col("point"), H3_LEVEL, False), 1),
        )
        .select("id_unidade", "total_enrollment", "point", "h3_cell")
    )


def neighbors_per_cell(schools: DataFrame) -> DataFrame:
    """Nº de escolas vizinhas por célula — igual para toda a célula.

    A vizinhança de uma célula é ela própria mais as adjacentes (k-ring 1). O
    total de escolas federais nessa vizinhança menos a própria escola dá o número
    de vizinhas, idêntico para todas as escolas da célula.
    """
    counts = schools.groupBy("h3_cell").agg(F.count(F.lit(1)).alias("cell_count"))
    expanded = counts.select(
        F.col("h3_cell").alias("center_cell"),
        F.explode(stf.ST_H3KRing(F.col("h3_cell"), 1, False)).alias("nb_cell"),
    )
    return (
        expanded.join(counts, expanded["nb_cell"] == counts["h3_cell"], "left")
        .groupBy("center_cell")
        .agg(
            (F.sum(F.coalesce("cell_count", F.lit(0))) - F.lit(1))
            .cast("long")
            .alias("n_federal_neighbors")
        )
    )


def avg_neighbor_distance_per_cell(schools: DataFrame) -> DataFrame:
    """Distância média (km) de cada escola às suas vizinhas, agregada por célula.

    Vizinhas = outras escolas federais em atividade na própria célula ou numa
    adjacente. Como todas as escolas de uma célula compartilham a mesma vizinhança,
    a média das médias por escola equivale à média de todos os pares da célula.
    Distância geodésica (``ST_DistanceSpheroid``, WGS84) sobre os pontos originais.
    """
    a = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("point").alias("a_point"),
    ).withColumn("nb_cell", F.explode(stf.ST_H3KRing(F.col("a_cell"), 1, False)))
    b = schools.select(
        F.col("id_unidade").alias("b_id"),
        F.col("h3_cell").alias("b_cell"),
        F.col("point").alias("b_point"),
    )
    pairs = a.join(b, a["nb_cell"] == b["b_cell"], "inner").filter(
        F.col("a_id") != F.col("b_id")
    )
    return pairs.groupBy("a_cell").agg(
        F.avg(
            stf.ST_DistanceSpheroid(F.col("a_point"), F.col("b_point")) / F.lit(1000.0)
        ).alias("avg_dist_neighbors_km")
    )


def avg_nearest_distance_per_cell(schools: DataFrame) -> DataFrame:
    """Distância média (km) à federal mais próxima (qualquer, sem restrição de célula).

    Para cada escola, a menor distância geodésica a qualquer outra federal em
    atividade; depois a média por célula. O universo de federais é pequeno, então
    o produto cartesiano (com broadcast) é viável.
    """
    a = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("point").alias("a_point"),
    )
    b = schools.select(
        F.col("id_unidade").alias("b_id"),
        F.col("point").alias("b_point"),
    )
    per_school = (
        a.crossJoin(F.broadcast(b))
        .filter(F.col("a_id") != F.col("b_id"))
        .withColumn(
            "dist_km",
            stf.ST_DistanceSpheroid(F.col("a_point"), F.col("b_point")) / F.lit(1000.0),
        )
        .groupBy("a_cell", "a_id")
        .agg(F.min("dist_km").alias("nearest_km"))
    )
    return per_school.groupBy("a_cell").agg(
        F.avg("nearest_km").alias("avg_dist_nearest_km")
    )


def main():
    """Grade H3 nível 5 das escolas federais em atividade, com matrículas e
    métricas de vizinhança. A geometria da célula é persistida em EPSG:3857."""
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("geoparquet").load(ORIGEM)
    schools = federal_active_schools(silver).cache()

    cell_base = schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("total_enrollment").cast("long").alias("total_enrollment"),
        F.avg("total_enrollment").alias("avg_enrollment"),
    )
    neighbors = neighbors_per_cell(schools)
    avg_neighbors = avg_neighbor_distance_per_cell(schools)
    nearest = avg_nearest_distance_per_cell(schools)

    cell_geom = stf.ST_Transform(
        F.element_at(stf.ST_H3ToGeom(F.array(cell_base["h3_cell"])), 1),
        F.lit("EPSG:4326"),
        F.lit("EPSG:3857"),
    )

    grid = (
        cell_base.join(
            neighbors, cell_base["h3_cell"] == neighbors["center_cell"], "left"
        )
        .join(avg_neighbors, cell_base["h3_cell"] == avg_neighbors["a_cell"], "left")
        .join(nearest, cell_base["h3_cell"] == nearest["a_cell"], "left")
        .select(
            cell_base["h3_cell"].alias("h3_cell"),
            "n_schools",
            "total_enrollment",
            "avg_enrollment",
            "n_federal_neighbors",
            "avg_dist_neighbors_km",
            "avg_dist_nearest_km",
            cell_geom.alias("geometry"),
        )
    )

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
