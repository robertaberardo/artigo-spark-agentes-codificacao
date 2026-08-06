from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
# k=1 (ST_H3KRing com exact_ring=False) = a própria célula + o anel imediatamente
# adjacente; é a definição de "vizinhança" pedida para as escolas.
NEIGHBOR_K = 1
M_PER_KM = 1000.0


def build_analysis_base(geo: DataFrame) -> DataFrame:
    """Ponto WGS84 original + célula H3 nível 5 de cada escola.

    O H3 é indexado sobre as coordenadas geográficas (não sobre a geometria 3857),
    condição para o id da célula ser correto.
    """
    return (
        geo.withColumn("geom_wgs84", stc.ST_Point(F.col("longitude"), F.col("latitude")))
        .transform(lambda d: dt.attach_h3_index(d, "geom_wgs84", H3_LEVEL, "h3_cell"))
        .select("id_unidade", "h3_cell", "geom_wgs84", "total_enrollment")
    )


def neighbor_distances(base: DataFrame) -> DataFrame:
    """Por escola: distância média às vizinhas e quantidade de vizinhas.

    Vizinhas = outras escolas na própria célula ou nas adjacentes (k-ring 1). O
    self-join espacial casa cada escola às escolas cuja célula está no seu k-ring,
    excluindo ela mesma; a distância é geodésica sobre os pontos WGS84 originais.
    """
    rings = base.select(
        F.col("id_unidade").alias("s_id"),
        F.col("geom_wgs84").alias("s_geom"),
        F.explode(
            stf.ST_H3KRing(F.col("h3_cell"), F.lit(NEIGHBOR_K), F.lit(False))
        ).alias("ring_cell"),
    )
    others = base.select(
        F.col("id_unidade").alias("t_id"),
        F.col("h3_cell").alias("t_cell"),
        F.col("geom_wgs84").alias("t_geom"),
    )
    is_neighbor = (F.col("ring_cell") == F.col("t_cell")) & (F.col("s_id") != F.col("t_id"))
    pairs = rings.join(others, is_neighbor, "inner").withColumn(
        "dist_m", stf.ST_DistanceSpheroid(F.col("s_geom"), F.col("t_geom"))
    )
    return pairs.groupBy("s_id").agg(
        F.avg("dist_m").alias("avg_dist_neighbors_m"),
        F.count(F.lit(1)).cast("long").alias("n_neighbors"),
    )


def nearest_federal(base: DataFrame) -> DataFrame:
    """Por escola: distância geodésica à federal mais próxima (qualquer célula).

    Produto cartesiano do conjunto (pequeno) de federais em atividade contra si
    mesmo, excluindo a própria escola; guarda a menor distância.
    """
    left = base.select(
        F.col("id_unidade").alias("s_id"), F.col("geom_wgs84").alias("s_geom")
    )
    right = base.select(
        F.col("id_unidade").alias("t_id"), F.col("geom_wgs84").alias("t_geom")
    )
    pairs = left.crossJoin(F.broadcast(right)).filter(F.col("s_id") != F.col("t_id"))
    with_dist = pairs.withColumn(
        "dist_m", stf.ST_DistanceSpheroid(F.col("s_geom"), F.col("t_geom"))
    )
    return with_dist.groupBy("s_id").agg(F.min("dist_m").alias("dist_nearest_m"))


def per_school_metrics(base: DataFrame) -> DataFrame:
    """Junta as métricas por escola; escolas sem vizinhas ficam com 0 vizinhas."""
    neighbors = neighbor_distances(base).withColumnRenamed("s_id", "id_unidade")
    nearest = nearest_federal(base).withColumnRenamed("s_id", "id_unidade")
    return (
        base.select("id_unidade", "h3_cell", "total_enrollment")
        .join(neighbors, "id_unidade", "left")
        .join(nearest, "id_unidade", "left")
        .withColumn("n_neighbors", F.coalesce(F.col("n_neighbors"), F.lit(0)))
    )


def grid_aggregations() -> list:
    """Agregações por célula H3.

    Vizinhas e distância média às vizinhas são idênticas para todas as escolas da
    mesma célula (compartilham o mesmo k-ring), então ``F.max``/``F.avg`` resumem a
    célula sem perda. A média de matrículas é total / nº de escolas.
    """
    return [
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("total_enrollment").cast("long").alias("total_enrollment"),
        (F.sum("total_enrollment") / F.count(F.lit(1))).cast("double").alias("avg_enrollment"),
        F.max("n_neighbors").cast("long").alias("n_federal_neighbors"),
        (F.avg("avg_dist_neighbors_m") / F.lit(M_PER_KM)).alias("avg_dist_neighbors_km"),
        (F.avg("dist_nearest_m") / F.lit(M_PER_KM)).alias("avg_dist_nearest_km"),
    ]


FINAL_COLUMNS = [
    "h3_cell",
    "n_schools",
    "total_enrollment",
    "avg_enrollment",
    "n_federal_neighbors",
    "avg_dist_neighbors_km",
    "avg_dist_nearest_km",
    "geometry",
]


def main():
    """Agrega as escolas federais ativas por célula H3 nível 5 (gold)."""
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    base = spark.read.format("geoparquet").load(ORIGEM).transform(build_analysis_base)

    grid = (
        per_school_metrics(base)
        .transform(lambda d: dt.aggregate_by_h3(d, "h3_cell", grid_aggregations()))
        .transform(dt.to_web_mercator)
        .select(*FINAL_COLUMNS)
    )

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
