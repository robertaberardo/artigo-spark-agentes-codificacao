from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5


def _to_km(geom_a: str, geom_b: str) -> "F.Column":
    """Distância geodésica (WGS84) entre dois pontos, em quilômetros."""
    return stf.ST_DistanceSpheroid(F.col(geom_a), F.col(geom_b)) / F.lit(1000.0)


def neighbor_stats(schools: DataFrame) -> DataFrame:
    """Por escola: nº de vizinhas e distância média (km) às vizinhas.

    Vizinhas são as outras escolas na própria célula H3 ou nas células
    imediatamente adjacentes (k-ring 1 = célula central + 6 anéis). Como o
    k-ring depende só da célula, todas as escolas de uma mesma célula têm o
    mesmo número de vizinhas.
    """
    left = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("geom_wgs84").alias("a_geom"),
        F.explode(
            stf.ST_H3KRing(F.col("h3_cell"), F.lit(1), F.lit(False))
        ).alias("ring_cell"),
    )
    right = schools.select(
        F.col("id_unidade").alias("b_id"),
        F.col("h3_cell").alias("b_cell"),
        F.col("geom_wgs84").alias("b_geom"),
    )
    pairs = (
        left.join(right, left["ring_cell"] == right["b_cell"], how="inner")
        .filter(F.col("a_id") != F.col("b_id"))
        .withColumn("dist_km", _to_km("a_geom", "b_geom"))
    )
    return pairs.groupBy(F.col("a_id").alias("id_unidade")).agg(
        F.count(F.lit(1)).cast("int").alias("n_neighbors"),
        F.avg("dist_km").alias("mean_dist_neighbors_km"),
    )


def nearest_stats(schools: DataFrame) -> DataFrame:
    """Por escola: distância (km) à federal em atividade mais próxima.

    Sem restrição de célula — considera todas as escolas federais em atividade,
    em qualquer lugar, exceto a própria.
    """
    a = schools.select(
        F.col("id_unidade").alias("a_id"), F.col("geom_wgs84").alias("a_geom")
    )
    b = schools.select(
        F.col("id_unidade").alias("b_id"), F.col("geom_wgs84").alias("b_geom")
    )
    pairs = (
        a.crossJoin(b)
        .filter(F.col("a_id") != F.col("b_id"))
        .withColumn("dist_km", _to_km("a_geom", "b_geom"))
    )
    return pairs.groupBy(F.col("a_id").alias("id_unidade")).agg(
        F.min("dist_km").alias("dist_nearest_km")
    )


def main():
    """Grade H3 nível 5 das escolas federais em atividade.

    Agrega por célula: contagem de escolas, matrículas totais e médias, número
    de vizinhas (constante na célula), distância média às vizinhas e distância
    média à federal mais próxima. Distâncias são geodésicas, medidas sobre as
    coordenadas WGS84 originais; a geometria da célula sai em EPSG:3857.
    """
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("geoparquet").load(ORIGEM)

    schools = (
        dt.add_point_geometry(silver, "latitude", "longitude", out_col="geom_wgs84")
        .transform(lambda d: dt.attach_h3_index(d, "geom_wgs84", H3_LEVEL, "h3_cell"))
        .select("id_unidade", "h3_cell", "enrollment", "geom_wgs84")
    ).cache()

    per_school = (
        schools.select("id_unidade", "h3_cell", "enrollment")
        .join(neighbor_stats(schools), on="id_unidade", how="left")
        .join(nearest_stats(schools), on="id_unidade", how="left")
        # sem vizinhas => 0 vizinhas (zero legítimo); a distância fica NULL.
        .withColumn("n_neighbors", F.coalesce(F.col("n_neighbors"), F.lit(0)))
    )

    grid = (
        per_school.groupBy("h3_cell")
        .agg(
            F.count(F.lit(1)).cast("long").alias("n_schools"),
            F.sum("enrollment").cast("long").alias("total_enrollment"),
            F.round(F.avg("enrollment"), 1).alias("avg_enrollment"),
            F.max("n_neighbors").cast("int").alias("n_federal_neighbors"),
            F.round(F.avg("mean_dist_neighbors_km"), 3).alias("avg_dist_neighbors_km"),
            F.round(F.avg("dist_nearest_km"), 3).alias("avg_dist_nearest_km"),
        )
        .withColumn(
            "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
        )
        .transform(dt.to_web_mercator)
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
