from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

# São vizinhas as escolas na própria célula ou nas imediatamente adjacentes
# (anel H3 de raio 1). As escolas já vêm indexadas em H3 nível 5 do silver.
NEIGHBOR_RING = 1


def main():
    """Agregação por célula H3 nível 5 das escolas federais em atividade."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.format("geoparquet").load(ORIGEM)

    # Distâncias geodésicas exigem o ponto em WGS84 (lat/long original), medido
    # antes de qualquer projeção — reconstrói o ponto a partir de lat/long.
    with_metrics = (
        escola.transform(
            lambda d: dt.add_point_geometry(d, "latitude", "longitude", "point_wgs84")
        )
        .transform(
            lambda d: dt.add_nearest_other_distance_km(
                d, "id_unidade", "point_wgs84", "dist_nearest_km"
            )
        )
        .transform(
            lambda d: dt.add_h3_ring_neighbor_stats(
                d,
                "id_unidade",
                "h3_cell",
                "point_wgs84",
                k=NEIGHBOR_RING,
                count_col="n_neighbors",
                dist_col="dist_neighbors_km",
            )
        )
    )

    # n_federal_neighbors é constante na célula (o anel é o mesmo para todas as
    # escolas dela), então F.max apenas materializa esse valor comum.
    hexbin = with_metrics.transform(
        lambda d: dt.aggregate_by_h3(
            d,
            "h3_cell",
            [
                F.count(F.lit(1)).cast("long").alias("n_schools"),
                F.sum("enrollment").cast("long").alias("total_enrollment"),
                F.round(F.avg("enrollment"), 1).alias("avg_enrollment"),
                F.max("n_neighbors").cast("long").alias("n_federal_neighbors"),
                F.round(F.avg("dist_neighbors_km"), 3).alias("avg_dist_neighbors_km"),
                F.round(F.avg("dist_nearest_km"), 3).alias("avg_dist_nearest_km"),
            ],
        )
    ).transform(dt.to_web_mercator).select(
        "h3_cell",
        "n_schools",
        "total_enrollment",
        "avg_enrollment",
        "n_federal_neighbors",
        "avg_dist_neighbors_km",
        "avg_dist_nearest_km",
        "geometry",
    )

    hexbin.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
