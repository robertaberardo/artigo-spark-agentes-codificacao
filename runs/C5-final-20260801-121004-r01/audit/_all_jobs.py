# === /d/runs/C5-final-20260801-121004-r01/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from sedona.spark import SedonaContext
from sedona.spark.sql import st_functions as stf

from utils import column_transforms as ct
from utils import dataframe_transforms as dt
from utils.metadata import load_metadata, table_location

ESCOLA_RAW = table_location("educacao", "escola", "raw")
MATRICULA_RAW = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

# H3 nível 5: hexágonos de ~250 km² de área média, escala regional.
H3_LEVEL = 5
# Disco k=1 (célula + 6 adjacentes) define a vizinhança de cada escola.
NEIGHBOR_K = 1

SETOR_FEDERAL = "1"  # SETOR: 1 federal, 2 estadual, 3 municipal, 4 privada
SITUACAO_ATIVA = "1"  # SITUACAO: 1 em atividade, 2 paralisada, 3 extinta

METERS_PER_KM = 1000.0
KM_DECIMALS = 3
ENROLLMENT_DECIMALS = 1


def raw_schema(table: str) -> StructType:
    """Monta o schema de leitura (tudo string) a partir do metadata da app.

    O CSV cru aterrissa todas as colunas como texto; a tipagem acontece no
    ``select`` seguinte. Ler o schema do metadata mantém a fonte de verdade única.
    """
    fields = load_metadata("educacao")["tables"][table]["raw"]["schema"]
    return StructType([StructField(f["name"], StringType()) for f in fields])


def read_federal_active_schools(spark) -> DataFrame:
    """Lê o raw de escolas e mantém só as federais em atividade com coordenada.

    Filtra por setor federal e situação "em atividade" e descarta pontos sem
    lat/long válida (fora do Brasil ou ausentes) — só assim entram no cálculo
    geoespacial.
    """
    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(raw_schema("escola"))
        .csv(ESCOLA_RAW)
    )
    is_federal = raw["SETOR"] == SETOR_FEDERAL
    is_active = raw["SITUACAO"] == SITUACAO_ATIVA
    escolas = raw.filter(is_federal & is_active).select(
        ct.zero_pad_code(raw["ID_UNIDADE"], 8).alias("id_unidade"),
        ct.to_coordinate(raw["LATITUDE"]).alias("lat"),
        ct.to_coordinate(raw["LONGITUDE"]).alias("lon"),
    )
    return escolas.transform(dt.filter_valid_coordinates)


def read_enrollment(spark) -> DataFrame:
    """Lê o raw de matrículas e devolve o total da educação básica por escola."""
    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(raw_schema("matricula"))
        .csv(MATRICULA_RAW)
    )
    return raw.select(
        ct.zero_pad_code(raw["ID_UNIDADE"], 8).alias("id_unidade"),
        raw["MAT_BASICA"].cast("int").alias("enrollment"),
    )


def build_schools(escolas: DataFrame, enrollment: DataFrame) -> DataFrame:
    """Junta matrículas, cria o ponto (WGS84) e indexa a célula H3 de cada escola."""
    joined = dt.join_no_fanout(escolas, enrollment, on="id_unidade", how="left")
    return joined.transform(
        lambda d: dt.add_point_geometry(d, "lat", "lon", "geom")
    ).transform(lambda d: dt.attach_h3_index(d, "geom", H3_LEVEL, "h3_cell"))


def neighbor_distances(schools: DataFrame) -> DataFrame:
    """Distância média de cada escola às suas vizinhas e nº de vizinhas.

    Vizinhas são as outras escolas na própria célula ou nas células adjacentes
    (disco H3 de raio ``NEIGHBOR_K``). Explode o disco de cada escola e cruza com
    as escolas cuja célula cai no disco, medindo a distância geodésica sobre as
    coordenadas WGS84 originais.
    """
    ring = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("geom").alias("a_geom"),
        F.explode(
            stf.ST_H3KRing(F.col("h3_cell"), F.lit(NEIGHBOR_K), F.lit(False))
        ).alias("ring_cell"),
    )
    others = schools.select(
        F.col("id_unidade").alias("b_id"),
        F.col("geom").alias("b_geom"),
        F.col("h3_cell").alias("b_cell"),
    )
    is_neighbor = (F.col("ring_cell") == F.col("b_cell")) & (
        F.col("a_id") != F.col("b_id")
    )
    pairs = ring.join(F.broadcast(others), is_neighbor, "inner").select(
        F.col("a_id").alias("id_unidade"),
        stf.ST_DistanceSpheroid(F.col("a_geom"), F.col("b_geom")).alias("dist_m"),
    )
    return pairs.groupBy("id_unidade").agg(
        F.avg("dist_m").alias("neighbor_dist_m"),
        F.count(F.lit(1)).cast("long").alias("n_neighbors"),
    )


def nearest_federal_distance(schools: DataFrame) -> DataFrame:
    """Distância de cada escola à federal mais próxima (qualquer, sem restrição de célula)."""
    left = schools.select(
        F.col("id_unidade").alias("a_id"), F.col("geom").alias("a_geom")
    )
    right = schools.select(
        F.col("id_unidade").alias("b_id"), F.col("geom").alias("b_geom")
    )
    pairs = left.crossJoin(F.broadcast(right)).filter(F.col("a_id") != F.col("b_id"))
    return (
        pairs.select(
            F.col("a_id").alias("id_unidade"),
            stf.ST_DistanceSpheroid(F.col("a_geom"), F.col("b_geom")).alias("dist_m"),
        )
        .groupBy("id_unidade")
        .agg(F.min("dist_m").alias("nearest_dist_m"))
    )


def aggregate_by_cell(
    schools: DataFrame, neighbors: DataFrame, nearest: DataFrame
) -> DataFrame:
    """Agrega as escolas por célula H3 e reconstrói a geometria da célula.

    ``n_federal_neighbors`` é uma propriedade da célula (todas as escolas de uma
    célula têm o mesmo número de vizinhas: universo da vizinhança menos a própria),
    então basta o valor por escola — constante dentro da célula.
    """
    per_school = (
        schools.select("id_unidade", "h3_cell", "enrollment")
        .join(neighbors, on="id_unidade", how="left")
        .join(nearest, on="id_unidade", how="left")
    )
    grid = per_school.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("enrollment").cast("long").alias("total_enrollment"),
        F.round(F.avg("enrollment"), ENROLLMENT_DECIMALS).alias("avg_enrollment"),
        F.max(F.coalesce(F.col("n_neighbors"), F.lit(0)))
        .cast("long")
        .alias("n_federal_neighbors"),
        F.round(F.avg("neighbor_dist_m") / F.lit(METERS_PER_KM), KM_DECIMALS).alias(
            "avg_dist_neighbors_km"
        ),
        F.round(F.avg("nearest_dist_m") / F.lit(METERS_PER_KM), KM_DECIMALS).alias(
            "avg_dist_nearest_km"
        ),
    )
    return grid.withColumn(
        "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
    )


def main():
    """Grade H3 nível 5 das escolas federais em atividade, com vizinhança e distâncias."""
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = build_schools(
        read_federal_active_schools(spark), read_enrollment(spark)
    ).cache()

    grid = aggregate_by_cell(
        schools, neighbor_distances(schools), nearest_federal_distance(schools)
    ).select(
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

