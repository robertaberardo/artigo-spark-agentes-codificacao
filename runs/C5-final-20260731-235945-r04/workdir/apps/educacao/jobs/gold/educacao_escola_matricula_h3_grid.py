from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_federal_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
# Vizinhança: própria célula + as imediatamente adjacentes (1 anel do ST_H3KRing).
K_RING = 1
METERS_PER_KM = 1000.0


def prepare_schools(df):
    """Recria o ponto WGS84 (para distância geodésica) e anexa a célula H3 nível 5.

    A geometria persistida no silver está em EPSG:3857; medir distância exige
    voltar ao WGS84 original, então o ponto é reconstruído a partir de
    latitude/longitude (sem round-trip de projeção), que também alimenta o H3.
    """
    return (
        df.transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude", "point"))
        .transform(lambda d: dt.attach_h3_index(d, "point", H3_LEVEL, "h3_cell"))
        .select("id_unidade", "h3_cell", "enrollment", "point")
    )


def add_distance_stats(df):
    """Anexa, por escola, as estatísticas de vizinhança e do vizinho mais próximo."""
    return (
        df.transform(
            lambda d: dt.add_kring_neighbor_stats(
                d, "id_unidade", "h3_cell", "point", K_RING,
                "n_neighbors", "avg_dist_neighbors_m",
            )
        ).transform(
            lambda d: dt.add_nearest_neighbor_distance(
                d, "id_unidade", "point", "dist_nearest_m"
            )
        )
    )


def cell_aggregations():
    """Expressões de agregação por célula (construídas com Spark já ativo)."""
    return [
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("enrollment").cast("long").alias("total_enrollment"),
        F.round(F.avg("enrollment"), 2).alias("avg_enrollment"),
        # n_neighbors é constante na célula (depende só da célula), logo max = o valor.
        F.max("n_neighbors").cast("long").alias("n_federal_neighbors"),
        F.round(F.avg("avg_dist_neighbors_km"), 3).alias("avg_dist_neighbors_km"),
        F.round(F.avg("dist_nearest_km"), 3).alias("avg_dist_nearest_km"),
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
    """Grade H3 nível 5 das escolas federais em atividade, com matrículas e distâncias."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("geoparquet").load(ORIGEM)

    per_school = (
        silver.transform(prepare_schools)
        .transform(add_distance_stats)
        .select(
            "h3_cell",
            "enrollment",
            "n_neighbors",
            (F.col("avg_dist_neighbors_m") / METERS_PER_KM).alias("avg_dist_neighbors_km"),
            (F.col("dist_nearest_m") / METERS_PER_KM).alias("dist_nearest_km"),
        )
    )

    grid = (
        dt.aggregate_by_h3(per_school, "h3_cell", cell_aggregations())
        .transform(dt.to_web_mercator)
        .select(*FINAL_COLUMNS)
    )

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    grid.drop("geometry").orderBy(F.col("n_schools").desc()).show(10, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
