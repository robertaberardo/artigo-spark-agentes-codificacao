"""Gold ``educacao_escola_matricula_h3_grid``.

Agrega, por célula da grade H3 nível 5, as escolas **federais em atividade**,
cruzando-as com as matrículas da educação básica. Para cada célula calcula a
quantidade de escolas, o total e a média de matrículas, a quantidade de escolas
vizinhas (na própria célula ou nas adjacentes — a mesma para toda a célula), a
distância média de cada escola às suas vizinhas e a distância média de cada
escola à federal mais próxima (qualquer, sem restrição de célula).

As distâncias são geodésicas (``ST_DistanceSpheroid``) medidas sobre os pontos
WGS84 ORIGINAIS; só o polígono da célula persistido é reprojetado para 3857.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from sedona.spark import SedonaContext
from sedona.spark.sql import st_functions as stf

from utils import column_transforms as ct
from utils import dataframe_transforms as dt
from utils.metadata import load_metadata, table_location

APP = "educacao"
META = load_metadata(APP)

ORIGEM_ESCOLA = table_location(APP, "escola", "raw", meta=META)
ORIGEM_MATRICULA = table_location(APP, "matricula", "raw", meta=META)
DESTINO = table_location(APP, "escola_matricula_h3_grid", "gold", meta=META)

H3_LEVEL = 5
NEIGHBOR_RING = 1  # anel 1 = própria célula + células imediatamente adjacentes
FEDERAL_SETOR = "1"  # setor administrativo: 1 = federal
ACTIVE_SITUACAO = "1"  # situação de funcionamento: 1 = em atividade
ID_WIDTH = 8  # ID_UNIDADE tem 8 dígitos
METERS_PER_KM = 1000.0


def raw_schema(table: str) -> T.StructType:
    """Monta o schema da camada raw a partir do metadata (raw aterrissa como texto)."""
    fields = META["tables"][table]["raw"]["schema"]
    return T.StructType([T.StructField(f["name"], T.StringType()) for f in fields])


def read_csv(spark: SparkSession, path: str, table: str) -> DataFrame:
    """Lê o CSV raw (``;``, com header) com o schema declarado no metadata."""
    return (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(raw_schema(table))
        .csv(path)
    )


def select_federal_active_schools(df: DataFrame) -> DataFrame:
    """Filtra escolas federais em atividade e prepara chave e coordenadas."""
    is_federal = F.col("SETOR") == FEDERAL_SETOR
    is_active = F.col("SITUACAO") == ACTIVE_SITUACAO
    return df.filter(is_federal & is_active).select(
        ct.zero_pad_code(F.col("ID_UNIDADE"), ID_WIDTH).alias("school_id"),
        ct.to_coordinate(F.col("LATITUDE")).alias("latitude"),
        ct.to_coordinate(F.col("LONGITUDE")).alias("longitude"),
    )


def build_school_points(df: DataFrame) -> DataFrame:
    """Mantém escolas com coordenada válida e anexa ponto WGS84 e célula H3."""
    return (
        df.transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(lambda d: dt.attach_h3_index(d, "geometry", H3_LEVEL, "h3_cell"))
        .select("school_id", "geometry", "h3_cell")
    )


def read_basic_enrollment(spark: SparkSession) -> DataFrame:
    """Matrículas na educação básica (``MAT_BASICA``) por escola.

    Ausência permanece ``NULL`` (não vira zero): o cast de string vazia já produz
    ``NULL`` e as agregações ignoram nulos.
    """
    raw = read_csv(spark, ORIGEM_MATRICULA, "matricula")
    return raw.select(
        ct.zero_pad_code(F.col("ID_UNIDADE"), ID_WIDTH).alias("school_id"),
        F.col("MAT_BASICA").cast(T.LongType()).alias("enrollment"),
    )


def geodesic_km(geom_a, geom_b):
    """Distância geodésica (km) entre dois pontos WGS84 via ``ST_DistanceSpheroid``."""
    return stf.ST_DistanceSpheroid(geom_a, geom_b) / F.lit(METERS_PER_KM)


def neighbor_distance_stats(schools: DataFrame) -> DataFrame:
    """Por escola: nº de vizinhas e distância média às vizinhas.

    Vizinhas são as demais escolas na própria célula ou nas adjacentes (anel H3
    de raio 1). O anel é o mesmo para toda a célula, logo o nº de vizinhas é
    uniforme entre as escolas de uma célula.
    """
    ring = schools.withColumn(
        "neighbor_cell",
        F.explode(stf.ST_H3KRing(F.col("h3_cell"), NEIGHBOR_RING, False)),
    )
    is_neighbor = (F.col("b.h3_cell") == F.col("a.neighbor_cell")) & (
        F.col("a.school_id") != F.col("b.school_id")
    )
    pairs = ring.alias("a").join(schools.alias("b"), is_neighbor, "inner")
    return (
        pairs.withColumn("dist_km", geodesic_km(F.col("a.geometry"), F.col("b.geometry")))
        .groupBy(F.col("a.school_id").alias("school_id"))
        .agg(
            F.count(F.lit(1)).alias("n_neighbors"),
            F.avg("dist_km").alias("avg_dist_neighbors_km"),
        )
    )


def nearest_distance_stats(schools: DataFrame) -> DataFrame:
    """Por escola: distância à federal mais próxima (qualquer, sem restrição de célula)."""
    others = F.broadcast(
        schools.select(
            F.col("school_id").alias("other_id"),
            F.col("geometry").alias("other_geometry"),
        )
    )
    pairs = (
        schools.crossJoin(others)
        .filter(F.col("school_id") != F.col("other_id"))
        .withColumn("dist_km", geodesic_km(F.col("geometry"), F.col("other_geometry")))
    )
    return pairs.groupBy("school_id").agg(F.min("dist_km").alias("dist_nearest_km"))


def aggregate_to_h3_grid(enriched: DataFrame) -> DataFrame:
    """Agrega as escolas por célula H3 e reconstrói o polígono da célula em 3857."""
    cell_geom = F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
    return (
        enriched.groupBy("h3_cell")
        .agg(
            F.count(F.lit(1)).alias("n_schools"),
            F.sum("enrollment").alias("total_enrollment"),
            F.round(F.avg("enrollment"), 2).alias("avg_enrollment"),
            F.max("n_neighbors").alias("n_federal_neighbors"),
            F.round(F.avg("avg_dist_neighbors_km"), 3).alias("avg_dist_neighbors_km"),
            F.round(F.avg("dist_nearest_km"), 3).alias("avg_dist_nearest_km"),
        )
        .withColumn(
            "geometry",
            stf.ST_Transform(cell_geom, F.lit("EPSG:4326"), F.lit("EPSG:3857")),
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


def main():
    """Constrói e persiste a grade H3 de escolas federais em atividade."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = (
        read_csv(spark, ORIGEM_ESCOLA, "escola")
        .transform(select_federal_active_schools)
        .transform(build_school_points)
        .cache()  # reusado nos ramos de vizinhança, mais próxima e agregação
    )
    schools.count()

    enrollment = read_basic_enrollment(spark)
    neighbors = neighbor_distance_stats(schools)
    nearest = nearest_distance_stats(schools)

    enriched = (
        schools
        .transform(lambda d: dt.join_no_fanout(d, enrollment, "school_id", "left"))
        .transform(lambda d: dt.join_no_fanout(d, neighbors, "school_id", "left"))
        .transform(lambda d: dt.join_no_fanout(d, nearest, "school_id", "left"))
        # escola sem vizinha tem contagem 0 (é uma contagem real, não ausência)
        .withColumn("n_neighbors", F.coalesce(F.col("n_neighbors"), F.lit(0)))
    )

    grid = aggregate_to_h3_grid(enriched)

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
