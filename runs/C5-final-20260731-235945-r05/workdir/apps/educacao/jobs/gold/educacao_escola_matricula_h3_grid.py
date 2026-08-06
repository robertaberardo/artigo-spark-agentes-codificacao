from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

# Anel de vizinhança: a própria célula + as imediatamente adjacentes (k=1).
NEIGHBOR_K = 1


def load_schools(spark) -> DataFrame:
    """Escolas federais em atividade com o ponto WGS84 reconstruído do lat/long.

    A distância geodésica precisa das coordenadas WGS84 originais; por isso o
    ponto é montado a partir de lat/long (``double``), sem reusar a geometria
    persistida em 3857 (evita o round-trip 3857→4326).
    """
    geo = spark.read.format("geoparquet").load(ORIGEM)
    return geo.select(
        "id_unidade",
        "h3_cell",
        "total_enrollment",
        stc.ST_Point(F.col("longitude"), F.col("latitude")).alias("wgs"),
    )


def neighbor_metrics(schools: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Contagem de vizinhas por célula e distância média às vizinhas por célula.

    Vizinhas são as outras escolas na própria célula ou nas adjacentes (anel H3
    k=1). O total de escolas do anel é igual para toda a célula, então cada
    escola tem ``pool - 1`` vizinhas (exclui a si mesma) — o valor de
    ``n_federal_neighbors``. As distâncias usam só os pares de escolas distintas.
    """
    left = schools.select(
        F.col("id_unidade").alias("sid"),
        F.col("h3_cell").alias("lcell"),
        F.col("wgs").alias("lgeom"),
    ).withColumn("ring_cell", F.explode(stf.ST_H3KRing(F.col("lcell"), NEIGHBOR_K, False)))
    right = schools.select(
        F.col("id_unidade").alias("rid"),
        F.col("h3_cell").alias("rcell"),
        F.col("wgs").alias("rgeom"),
    )
    pairs = left.join(right, F.col("ring_cell") == F.col("rcell"), "inner")

    pool = pairs.groupBy("lcell").agg(
        (F.count_distinct("rid") - F.lit(1)).cast("long").alias("n_federal_neighbors")
    ).withColumnRenamed("lcell", "h3_cell")

    # Distância geodésica (m) entre pares de escolas distintas, no WGS84 original.
    neighbors = pairs.filter(F.col("sid") != F.col("rid")).withColumn(
        "dist_m", stf.ST_DistanceSpheroid(F.col("lgeom"), F.col("rgeom"))
    )
    per_school = neighbors.groupBy("sid", "lcell").agg(
        F.avg("dist_m").alias("school_avg_dist_m")
    )
    per_cell = per_school.groupBy("lcell").agg(
        F.avg("school_avg_dist_m").alias("avg_dist_neighbors_m")
    ).withColumnRenamed("lcell", "h3_cell")
    return pool, per_cell


def nearest_metrics(schools: DataFrame) -> DataFrame:
    """Distância média, por célula, de cada escola à federal mais próxima.

    Sem restrição de célula: para cada escola busca a menor distância geodésica
    a qualquer outra federal em atividade, depois faz a média por célula.
    """
    a = schools.select(
        F.col("id_unidade").alias("aid"),
        F.col("h3_cell").alias("acell"),
        F.col("wgs").alias("ageom"),
    )
    b = schools.select(
        F.col("id_unidade").alias("bid"),
        F.col("wgs").alias("bgeom"),
    )
    pairs = a.crossJoin(b).filter(F.col("aid") != F.col("bid")).withColumn(
        "dist_m", stf.ST_DistanceSpheroid(F.col("ageom"), F.col("bgeom"))
    )
    per_school = pairs.groupBy("aid", "acell").agg(F.min("dist_m").alias("nearest_m"))
    return per_school.groupBy("acell").agg(
        F.avg("nearest_m").alias("avg_dist_nearest_m")
    ).withColumnRenamed("acell", "h3_cell")


def main():
    """Grade H3 nível 5 das escolas federais em atividade (contagem + distâncias)."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = load_schools(spark).cache()

    base = schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("total_enrollment").cast("long").alias("total_enrollment"),
        F.avg("total_enrollment").alias("avg_enrollment"),
    )
    pool, neighbors = neighbor_metrics(schools)
    nearest = nearest_metrics(schools)

    grid = (
        base.transform(lambda d: dt.join_no_fanout(d, pool, "h3_cell", "left"))
        .transform(lambda d: dt.join_no_fanout(d, neighbors, "h3_cell", "left"))
        .transform(lambda d: dt.join_no_fanout(d, nearest, "h3_cell", "left"))
        .withColumn(
            "geometry",
            stf.ST_Transform(
                F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1),
                F.lit("EPSG:4326"),
                F.lit("EPSG:3857"),
            ),
        )
    )

    result = grid.select(
        F.col("h3_cell"),
        F.col("n_schools"),
        F.col("total_enrollment"),
        F.round(F.col("avg_enrollment"), 2).alias("avg_enrollment"),
        F.coalesce(F.col("n_federal_neighbors"), F.lit(0)).alias("n_federal_neighbors"),
        F.round(F.col("avg_dist_neighbors_m") / F.lit(1000.0), 3).alias("avg_dist_neighbors_km"),
        F.round(F.col("avg_dist_nearest_m") / F.lit(1000.0), 3).alias("avg_dist_nearest_km"),
        F.col("geometry"),
    )

    result.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
