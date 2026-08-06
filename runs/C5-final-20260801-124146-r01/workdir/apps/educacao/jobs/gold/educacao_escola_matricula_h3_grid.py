from pyspark.sql import DataFrame, functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5              # resolução da grade H3
SETOR_FEDERAL = 1
SITUACAO_ATIVA = 1
KRING_K = 1              # vizinhança: a própria célula + as adjacentes imediatas
METERS_PER_KM = 1000.0

COLUNAS = [
    "h3_cell",
    "n_schools",
    "total_enrollment",
    "avg_enrollment",
    "n_federal_neighbors",
    "avg_dist_neighbors_km",
    "avg_dist_nearest_km",
    "geometry",
]


def federal_ativa_base(spark) -> DataFrame:
    """Escolas federais em atividade, com ponto WGS84 e célula H3 nível 5.

    H3 e distância geodésica operam sobre a lat/long WGS84 original (não sobre a
    geometria 3857 do silver): reconstrói o ponto a partir de latitude/longitude.
    """
    silver = spark.read.format("geoparquet").load(ORIGEM)
    return (
        silver.filter(
            (F.col("setor") == SETOR_FEDERAL) & (F.col("situacao") == SITUACAO_ATIVA)
        )
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude", "geom_wgs"))
        .transform(lambda d: dt.attach_h3_index(d, "geom_wgs", H3_LEVEL, "h3_cell"))
        .select("id_unidade", "h3_cell", "matriculas", "geom_wgs")
    )


def cell_aggregates(base: DataFrame) -> DataFrame:
    """Escolas, matrículas e polígono H3 (em 3857) por célula."""
    return (
        base.groupBy("h3_cell")
        .agg(
            F.count(F.lit(1)).cast("long").alias("n_schools"),
            F.sum("matriculas").cast("long").alias("total_enrollment"),
            F.avg("matriculas").alias("avg_enrollment"),
        )
        .withColumn(
            "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
        )
        .transform(dt.to_web_mercator)  # polígono H3 (4326) -> 3857, convenção de saída
    )


def cell_kring(base: DataFrame) -> DataFrame:
    """Para cada célula, o anel-1 de células (a própria + as adjacentes)."""
    return base.select("h3_cell").distinct().withColumn(
        "kring", stf.ST_H3KRing(F.col("h3_cell"), KRING_K, False)
    )


def neighbor_counts(base: DataFrame, kring: DataFrame) -> DataFrame:
    """Nº de escolas federais na vizinhança da célula (constante por célula).

    Soma, sobre a própria célula e as adjacentes, quantas escolas cada uma tem.
    """
    per_cell = base.groupBy("h3_cell").agg(F.count(F.lit(1)).alias("cnt"))
    exploded = kring.select("h3_cell", F.explode("kring").alias("neighbor_cell"))
    return (
        exploded.join(
            per_cell.withColumnRenamed("h3_cell", "neighbor_cell"),
            on="neighbor_cell",
            how="left",
        )
        .groupBy("h3_cell")
        .agg(F.sum("cnt").cast("long").alias("n_federal_neighbors"))
    )


def distance_aggregates(base: DataFrame, kring: DataFrame) -> DataFrame:
    """Distâncias geodésicas por escola, agregadas por célula.

    Um self cross-join (conjunto pequeno) dá, para cada escola: a distância à
    federal mais próxima (qualquer célula) e a média às vizinhas (mesma célula ou
    adjacente). A média por célula ignora escolas sem vizinha (NULL).
    """
    base_k = base.join(kring, on="h3_cell", how="inner")
    left = base_k.alias("s")
    right = base.alias("t")

    dist_km = stf.ST_DistanceSpheroid(
        F.col("s.geom_wgs"), F.col("t.geom_wgs")
    ) / F.lit(METERS_PER_KM)
    is_neighbor = F.array_contains(F.col("s.kring"), F.col("t.h3_cell"))

    pairs = left.join(
        right, F.col("s.id_unidade") != F.col("t.id_unidade")
    ).select(
        F.col("s.id_unidade").alias("id_unidade"),
        F.col("s.h3_cell").alias("h3_cell"),
        dist_km.alias("dist_km"),
        F.when(is_neighbor, dist_km).alias("neighbor_dist_km"),
    )

    per_school = pairs.groupBy("id_unidade", "h3_cell").agg(
        F.min("dist_km").alias("nearest_km"),
        F.avg("neighbor_dist_km").alias("neighbor_mean_km"),
    )
    return per_school.groupBy("h3_cell").agg(
        F.avg("neighbor_mean_km").alias("avg_dist_neighbors_km"),
        F.avg("nearest_km").alias("avg_dist_nearest_km"),
    )


def main():
    """Grade H3 nível 5 das escolas federais em atividade."""
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    base = federal_ativa_base(spark).cache()  # reusado em todos os passos
    kring = cell_kring(base)

    grid = (
        cell_aggregates(base)
        .join(neighbor_counts(base, kring), on="h3_cell", how="left")
        .join(distance_aggregates(base, kring), on="h3_cell", how="left")
        .select(*COLUNAS)
    )

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
