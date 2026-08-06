from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
KRING_ADJACENTES = 1
METERS_PER_KM = 1000.0


def index_schools(df: DataFrame) -> DataFrame:
    """Anexa o ponto WGS84 e a célula H3 nível 5 de cada escola.

    O ponto é reconstruído das coordenadas originais (não da geometria 3857 do
    silver): tanto o H3 quanto ``ST_DistanceSpheroid`` operam sobre WGS84, e usar
    a geometria métrica atribuiria células erradas e mediria distâncias tortas.
    """
    with_point = df.withColumn(
        "point", stc.ST_Point(F.col("longitude"), F.col("latitude"))
    )
    return with_point.withColumn(
        "h3_cell", F.element_at(stf.ST_H3CellIDs(F.col("point"), H3_LEVEL, False), 1)
    )


def cell_basic_stats(schools: DataFrame) -> DataFrame:
    """Contagem de escolas, total e média de matrículas por célula."""
    return schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("enrollment").cast("long").alias("total_enrollment"),
        F.round(F.avg("enrollment"), 2).alias("avg_enrollment"),
    )


def cell_nearest_stats(schools: DataFrame) -> DataFrame:
    """Distância média à federal mais próxima (qualquer, sem restrição de célula).

    Para cada escola mede a distância geodésica a todas as demais e fica com a
    menor; depois tira a média por célula.
    """
    left = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell"),
        F.col("point").alias("a_point"),
    )
    right = schools.select(
        F.col("id_unidade").alias("b_id"), F.col("point").alias("b_point")
    )
    per_school = (
        left.crossJoin(right)
        .filter(F.col("a_id") != F.col("b_id"))
        .withColumn("dist_m", stf.ST_DistanceSpheroid(F.col("a_point"), F.col("b_point")))
        .groupBy("a_id", "h3_cell")
        .agg(F.min("dist_m").alias("nearest_m"))
    )
    return per_school.groupBy("h3_cell").agg(
        F.round(F.avg("nearest_m") / METERS_PER_KM, 3).alias("avg_dist_nearest_km")
    )


def _disk_schools(schools: DataFrame) -> DataFrame:
    """Escolas do disco (célula + adjacentes) de cada célula focal.

    Expande cada célula focal em seu k-ring 1 (a própria célula e as seis
    vizinhas) e casa com as escolas situadas nessas células.
    """
    cell_disk = (
        schools.select("h3_cell")
        .distinct()
        .withColumn(
            "disk_cell",
            F.explode(stf.ST_H3KRing(F.col("h3_cell"), KRING_ADJACENTES, False)),
        )
    )
    neighbors = schools.select(
        F.col("h3_cell").alias("disk_cell"),
        F.col("id_unidade").alias("nb_id"),
        F.col("point").alias("nb_point"),
    )
    return cell_disk.join(neighbors, on="disk_cell", how="inner")


def cell_neighbor_stats(schools: DataFrame) -> DataFrame:
    """Quantidade de vizinhas e distância média de cada escola às suas vizinhas.

    Vizinhas de uma escola são as demais federais em atividade da própria célula
    ou das adjacentes. O disco é idêntico para todas as escolas da célula, então
    a contagem (escolas do disco menos a própria) é a mesma para toda a célula.
    """
    disk_schools = _disk_schools(schools)

    counts = disk_schools.groupBy("h3_cell").agg(
        (F.countDistinct("nb_id") - F.lit(1)).cast("long").alias("n_federal_neighbors")
    )

    focal = schools.select(
        "h3_cell",
        F.col("id_unidade").alias("f_id"),
        F.col("point").alias("f_point"),
    )
    distances = (
        focal.join(
            disk_schools.select("h3_cell", "nb_id", "nb_point"), on="h3_cell", how="inner"
        )
        .filter(F.col("f_id") != F.col("nb_id"))
        .withColumn("dist_m", stf.ST_DistanceSpheroid(F.col("f_point"), F.col("nb_point")))
        .groupBy("h3_cell")
        .agg(F.round(F.avg("dist_m") / METERS_PER_KM, 3).alias("avg_dist_neighbors_km"))
    )
    return counts.join(distances, on="h3_cell", how="left")


def main():
    """Grade H3 nível 5 de escolas federais em atividade, com vizinhança."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = index_schools(spark.read.format("geoparquet").load(ORIGEM)).cache()

    grid = (
        cell_basic_stats(schools)
        .join(cell_neighbor_stats(schools), on="h3_cell", how="left")
        .join(cell_nearest_stats(schools), on="h3_cell", how="left")
        .withColumn(
            "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
        )
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
