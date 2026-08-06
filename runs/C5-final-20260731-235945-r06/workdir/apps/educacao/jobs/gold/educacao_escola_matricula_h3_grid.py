from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
KRING_K = 1  # células imediatamente adjacentes (anel de distância 1)
METERS_PER_KM = 1000.0


def prepare_schools(spark) -> DataFrame:
    """Lê o silver e prepara ponto WGS84, célula H3 res 5 e vizinhança de células.

    Reconstrói o ponto a partir de ``latitude``/``longitude`` originais (não da
    geometria 3857 do silver) para medir distâncias geodésicas sem o round-trip de
    reprojeção. ``neighbor_cells`` são as células do k-ring (própria + adjacentes).
    """
    geo = spark.read.format("geoparquet").load(ORIGEM)
    return (
        geo.transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(lambda d: dt.attach_h3_index(d, "geometry", H3_LEVEL, "h3_cell"))
        .withColumn(
            "neighbor_cells",
            stf.ST_H3KRing(F.col("h3_cell"), F.lit(KRING_K), F.lit(False)),
        )
        .select(
            "id_unidade", "h3_cell", "neighbor_cells", "total_enrollment", "geometry"
        )
    )


def pairwise_school_stats(schools: DataFrame) -> DataFrame:
    """Estatísticas por escola a partir dos pares (escola, outra escola federal).

    Um único cross-join (poucas centenas de federais) fornece as duas medidas:
    a distância à federal mais próxima (mínimo sobre todos os pares) e as métricas
    de vizinhança (pares cujas células caem no k-ring da escola). Escolas isoladas
    ficam com ``n_neighbors=0`` e ``mean_dist_neighbors_km`` nulo.
    """
    left = schools.select(
        "id_unidade", "h3_cell", "neighbor_cells", F.col("geometry").alias("geom")
    )
    right = schools.select(
        F.col("id_unidade").alias("other_id"),
        F.col("h3_cell").alias("other_cell"),
        F.col("geometry").alias("other_geom"),
    )

    dist_km = stf.ST_DistanceSpheroid(F.col("geom"), F.col("other_geom")) / F.lit(
        METERS_PER_KM
    )
    is_neighbor = F.array_contains(F.col("neighbor_cells"), F.col("other_cell"))

    pairs = (
        left.crossJoin(right)
        .filter(F.col("id_unidade") != F.col("other_id"))
        .withColumn("dist_km", dist_km)
        .withColumn("is_neighbor", is_neighbor)
    )

    return pairs.groupBy("id_unidade", "h3_cell").agg(
        F.min("dist_km").alias("nearest_dist_km"),
        F.sum(F.when(F.col("is_neighbor"), F.lit(1)).otherwise(F.lit(0))).alias(
            "n_neighbors"
        ),
        F.avg(F.when(F.col("is_neighbor"), F.col("dist_km"))).alias(
            "mean_dist_neighbors_km"
        ),
    )


def aggregate_cells(schools: DataFrame, per_school: DataFrame) -> DataFrame:
    """Agrega escolas e métricas por célula H3 e reconstrói o polígono da célula.

    ``n_federal_neighbors`` é constante dentro da célula (toda escola exclui apenas
    a si mesma do anel), então ``max`` recupera esse valor único. As médias de
    distância ignoram nulos de escolas sem vizinhas. A geometria da célula sai em
    EPSG:3857.
    """
    base = schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("total_enrollment").cast("long").alias("total_enrollment"),
        F.round(F.avg("total_enrollment"), 2).alias("avg_enrollment"),
    )
    neighbors = per_school.groupBy("h3_cell").agg(
        F.max("n_neighbors").cast("long").alias("n_federal_neighbors"),
        F.round(F.avg("mean_dist_neighbors_km"), 3).alias("avg_dist_neighbors_km"),
        F.round(F.avg("nearest_dist_km"), 3).alias("avg_dist_nearest_km"),
    )

    combined = base.join(neighbors, on="h3_cell", how="left").withColumn(
        "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
    )
    return combined.transform(dt.to_web_mercator).select(
        "h3_cell",
        "n_schools",
        "total_enrollment",
        "avg_enrollment",
        F.coalesce(F.col("n_federal_neighbors"), F.lit(0)).alias("n_federal_neighbors"),
        "avg_dist_neighbors_km",
        "avg_dist_nearest_km",
        "geometry",
    )


def main():
    """Agregação por célula H3 res 5 das escolas federais em atividade."""
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = prepare_schools(spark)
    per_school = pairwise_school_stats(schools)
    grid = aggregate_cells(schools, per_school)

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
