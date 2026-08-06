from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import column_transforms as ct
from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
METERS_PER_KM = 1000.0


def add_wgs84_point(df: DataFrame) -> DataFrame:
    """Reconstrói o ponto em WGS84 a partir de lat/long originais.

    A geometria persistida no silver está em EPSG:3857; para medir distâncias e
    indexar em H3 precisamos das coordenadas geográficas originais, sem o
    round-trip de reprojeção que perderia precisão.
    """
    return df.withColumn(
        "point_wgs84", ct.make_point(F.col("longitude"), F.col("latitude"))
    )


def main():
    """Grade H3 nível 5 das escolas federais em atividade.

    Por célula: nº de escolas, total e média de matrículas, nº de escolas vizinhas
    (na própria célula ou nas adjacentes — igual para toda a célula), distância
    média de cada escola às suas vizinhas e distância média de cada escola à
    federal mais próxima (qualquer, sem restrição de célula). As distâncias são
    geodésicas (``ST_DistanceSpheroid``) sobre as coordenadas WGS84 originais; só
    a geometria da célula é persistida em EPSG:3857.
    """
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = (
        spark.read.format("geoparquet").load(ORIGEM)
        .transform(add_wgs84_point)
        .transform(lambda d: dt.attach_h3_index(d, "point_wgs84", H3_LEVEL, "h3_cell"))
        .transform(
            lambda d: dt.add_nearest_neighbor_distance(
                d, "id_unidade", "point_wgs84", "nearest_dist_m"
            )
        )
        .transform(
            lambda d: dt.add_h3_ring_neighbor_stats(
                d, "id_unidade", "h3_cell", "point_wgs84", 1,
                "n_ring_neighbors", "avg_ring_dist_m",
            )
        )
    )

    grid = dt.aggregate_by_h3(
        schools,
        "h3_cell",
        [
            F.count(F.lit(1)).cast("long").alias("n_schools"),
            F.sum("total_enrollment").cast("long").alias("total_enrollment"),
            F.round(F.avg("total_enrollment"), 2).alias("avg_enrollment"),
            # a contagem de vizinhas é constante dentro da célula (depende só dela)
            F.max("n_ring_neighbors").cast("long").alias("n_federal_neighbors"),
            F.round(F.avg("avg_ring_dist_m") / METERS_PER_KM, 3).alias(
                "avg_dist_neighbors_km"
            ),
            F.round(F.avg("nearest_dist_m") / METERS_PER_KM, 3).alias(
                "avg_dist_nearest_km"
            ),
        ],
    ).transform(dt.to_web_mercator)

    grid = grid.select(
        "h3_cell",
        "n_schools",
        "total_enrollment",
        "avg_enrollment",
        "n_federal_neighbors",
        "avg_dist_neighbors_km",
        "avg_dist_nearest_km",
        "geometry",
    )

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
