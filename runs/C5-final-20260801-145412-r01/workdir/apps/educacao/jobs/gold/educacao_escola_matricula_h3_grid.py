from pyspark.sql import DataFrame, functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
SETOR_FEDERAL = 1
SITUACAO_ATIVA = 1


def load_federal_active_schools(spark) -> DataFrame:
    """Escolas federais em atividade, com ponto WGS84 e célula H3 de resolução 5.

    ``pt`` fica em WGS84 (a partir da lat/long originais) para a distância
    geodésica; a célula H3 é calculada em coordenadas geográficas.
    """
    geo = spark.read.format("geoparquet").load(ORIGEM)
    is_federal = F.col("setor") == SETOR_FEDERAL
    is_active = F.col("situacao") == SITUACAO_ATIVA
    return (
        geo.filter(is_federal & is_active)
        .withColumn("pt", stc.ST_Point(F.col("longitude"), F.col("latitude")))
        .withColumn("h3_cell", F.element_at(stf.ST_H3CellIDs(F.col("pt"), H3_LEVEL, False), 1))
        .select("id_unidade", "enrollment", "pt", "h3_cell")
    )


def cell_enrollment_stats(schools: DataFrame) -> DataFrame:
    """Contagem de escolas e estatísticas de matrículas por célula."""
    return schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("int").alias("n_schools"),
        F.sum("enrollment").cast("long").alias("total_enrollment"),
        F.avg("enrollment").alias("avg_enrollment"),
    )


def cell_neighbor_counts(schools: DataFrame) -> DataFrame:
    """Escolas na célula + adjacentes (disco k=1) — igual para toda a célula."""
    counts = schools.groupBy("h3_cell").agg(F.count(F.lit(1)).alias("cnt"))
    rings = schools.select("h3_cell").distinct().withColumn(
        "ring_cell", F.explode(stf.ST_H3KRing(F.col("h3_cell"), 1, False))
    )
    return (
        rings.alias("r")
        .join(counts.alias("c"), F.col("r.ring_cell") == F.col("c.h3_cell"), "left")
        .groupBy(F.col("r.h3_cell").alias("h3_cell"))
        .agg(F.sum(F.coalesce(F.col("c.cnt"), F.lit(0))).cast("int").alias("n_federal_neighbors"))
    )


def _neighbor_pairs(schools: DataFrame) -> DataFrame:
    """Pares (escola, vizinha) com a distância geodésica em km entre elas.

    Vizinha = outra escola cuja célula está no disco k=1 (própria + adjacentes)
    da célula da escola. A distância é medida sobre os pontos WGS84 originais.
    """
    a = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("pt").alias("a_pt"),
    ).withColumn("ring_cell", F.explode(stf.ST_H3KRing(F.col("a_cell"), 1, False)))
    b = schools.select(
        F.col("id_unidade").alias("b_id"),
        F.col("h3_cell").alias("b_cell"),
        F.col("pt").alias("b_pt"),
    )
    return (
        a.join(b, F.col("ring_cell") == F.col("b_cell"), "inner")
        .filter(F.col("a_id") != F.col("b_id"))
        .withColumn(
            "dist_km", stf.ST_DistanceSpheroid(F.col("a_pt"), F.col("b_pt")) / F.lit(1000.0)
        )
    )


def cell_neighbor_distances(schools: DataFrame) -> DataFrame:
    """Distância média de cada escola às vizinhas, agregada por célula."""
    per_school = _neighbor_pairs(schools).groupBy("a_id", "a_cell").agg(
        F.avg("dist_km").alias("school_avg_km")
    )
    return per_school.groupBy(F.col("a_cell").alias("h3_cell")).agg(
        F.avg("school_avg_km").alias("avg_dist_neighbors_km")
    )


def cell_nearest_distances(schools: DataFrame) -> DataFrame:
    """Distância média de cada escola à federal mais próxima (qualquer célula)."""
    a = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("pt").alias("a_pt"),
    )
    other = schools.select(
        F.col("id_unidade").alias("o_id"), F.col("pt").alias("o_pt")
    )
    per_school = (
        a.crossJoin(other)
        .filter(F.col("a_id") != F.col("o_id"))
        .withColumn(
            "dist_km", stf.ST_DistanceSpheroid(F.col("a_pt"), F.col("o_pt")) / F.lit(1000.0)
        )
        .groupBy("a_id", "a_cell")
        .agg(F.min("dist_km").alias("nearest_km"))
    )
    return per_school.groupBy(F.col("a_cell").alias("h3_cell")).agg(
        F.avg("nearest_km").alias("avg_dist_nearest_km")
    )


def main():
    """Grade H3 res.5 de escolas federais em atividade: densidade + distâncias."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = load_federal_active_schools(spark).cache()

    grid = (
        cell_enrollment_stats(schools)
        .join(cell_neighbor_counts(schools), "h3_cell", "left")
        .join(cell_neighbor_distances(schools), "h3_cell", "left")
        .join(cell_nearest_distances(schools), "h3_cell", "left")
        .withColumn("geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1))
        .transform(dt.to_web_mercator)
        .select(
            "h3_cell",
            "n_schools",
            "total_enrollment",
            F.round("avg_enrollment", 2).alias("avg_enrollment"),
            "n_federal_neighbors",
            F.round("avg_dist_neighbors_km", 3).alias("avg_dist_neighbors_km"),
            F.round("avg_dist_nearest_km", 3).alias("avg_dist_nearest_km"),
            "geometry",
        )
    )

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
