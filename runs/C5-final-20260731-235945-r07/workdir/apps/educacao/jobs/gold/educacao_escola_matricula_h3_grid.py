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
SETOR_FEDERAL = "1"
SITUACAO_ATIVA = "1"


def federal_active_schools(df: DataFrame) -> DataFrame:
    """Escolas federais em atividade, com ponto WGS84 e célula H3 nível 5.

    Filtra setor federal e situação em atividade, reconstrói o ponto original em
    EPSG:4326 a partir de ``latitude``/``longitude`` (para medir distância
    geodésica sem passar pelo 3857) e anexa a célula H3 (nível 5) que o contém.
    """
    ponto_4326 = stc.ST_Point(F.col("longitude"), F.col("latitude"))
    return (
        df.filter(
            (F.col("setor") == SETOR_FEDERAL) & (F.col("situacao") == SITUACAO_ATIVA)
        )
        .withColumn("geom_4326", ponto_4326)
        .transform(lambda d: dt.attach_h3_index(d, "geom_4326", H3_LEVEL, "h3_cell"))
        .select("id_unidade", "h3_cell", "total_enrollment", "geom_4326")
    )


def cell_neighbor_counts(schools: DataFrame) -> DataFrame:
    """Nº de escolas federais vizinhas por célula (mesmo valor para toda a célula).

    A vizinhança de uma célula é ela própria mais as células imediatamente
    adjacentes (``ST_H3KRing`` com k=1). O total de escolas nessa vizinhança é
    ``N``; como uma escola não é vizinha de si mesma, cada escola da célula tem
    ``N - 1`` vizinhas — número idêntico para todas as escolas da célula.
    """
    cell_counts = schools.groupBy("h3_cell").agg(F.count(F.lit(1)).alias("cnt"))
    neighborhood = cell_counts.select(
        "h3_cell",
        F.explode(stf.ST_H3KRing(F.col("h3_cell"), F.lit(1), F.lit(False))).alias(
            "neighbor_cell"
        ),
    )
    lookup = cell_counts.select(
        F.col("h3_cell").alias("neighbor_cell"), F.col("cnt").alias("neighbor_cnt")
    )
    return (
        neighborhood.join(lookup, on="neighbor_cell", how="left")
        .groupBy("h3_cell")
        .agg(
            (F.sum(F.coalesce("neighbor_cnt", F.lit(0))) - F.lit(1))
            .cast("long")
            .alias("n_federal_neighbors")
        )
    )


def cell_neighbor_distance(schools: DataFrame) -> DataFrame:
    """Distância média de cada escola às suas vizinhas, agregada por célula (km).

    Para cada escola A, as vizinhas são as demais escolas B situadas na célula de
    A ou nas adjacentes (``ST_H3KRing`` k=1). Mede a distância geodésica A–B
    (``ST_DistanceSpheroid`` sobre o ponto WGS84), tira a média por escola e, em
    seguida, a média dessas médias por célula. Escolas sem vizinha não entram.
    """
    a = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("geom_4326").alias("a_geom"),
    ).withColumn(
        "neighbor_cell",
        F.explode(stf.ST_H3KRing(F.col("a_cell"), F.lit(1), F.lit(False))),
    )
    b = schools.select(
        F.col("id_unidade").alias("b_id"),
        F.col("h3_cell").alias("neighbor_cell"),
        F.col("geom_4326").alias("b_geom"),
    )
    pairs = (
        a.join(b, on="neighbor_cell", how="inner")
        .filter(F.col("a_id") != F.col("b_id"))
        .withColumn("dist_m", stf.ST_DistanceSpheroid(F.col("a_geom"), F.col("b_geom")))
    )
    per_school = pairs.groupBy("a_cell", "a_id").agg(
        F.avg("dist_m").alias("avg_dist_school_m")
    )
    return per_school.groupBy("a_cell").agg(
        F.round(F.avg("avg_dist_school_m") / F.lit(1000.0), 3).alias(
            "avg_dist_neighbors_km"
        )
    ).withColumnRenamed("a_cell", "h3_cell")


def cell_nearest_distance(schools: DataFrame) -> DataFrame:
    """Distância média de cada escola à federal mais próxima (qualquer), por célula.

    Sem restrição de célula: para cada escola A, busca a menor distância geodésica
    a qualquer outra escola federal em atividade (produto cartesiano; o conjunto de
    federais é pequeno e vai em ``broadcast``), depois tira a média por célula (km).
    """
    a = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("geom_4326").alias("a_geom"),
    )
    b = schools.select(
        F.col("id_unidade").alias("b_id"),
        F.col("geom_4326").alias("b_geom"),
    )
    nearest_per_school = (
        a.crossJoin(F.broadcast(b))
        .filter(F.col("a_id") != F.col("b_id"))
        .withColumn("dist_m", stf.ST_DistanceSpheroid(F.col("a_geom"), F.col("b_geom")))
        .groupBy("a_cell", "a_id")
        .agg(F.min("dist_m").alias("nearest_m"))
    )
    return nearest_per_school.groupBy("a_cell").agg(
        F.round(F.avg("nearest_m") / F.lit(1000.0), 3).alias("avg_dist_nearest_km")
    ).withColumnRenamed("a_cell", "h3_cell")


def main():
    """Grade H3 nível 5 das escolas federais em atividade.

    Para cada célula: nº de escolas, total e média de matrículas, nº de vizinhas
    federais, distância média às vizinhas e distância média à federal mais próxima.
    A geometria de saída é o polígono da célula em EPSG:3857.
    """
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("geoparquet").load(ORIGEM)
    schools = federal_active_schools(silver).cache()

    base = schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("total_enrollment").cast("long").alias("total_enrollment"),
        F.round(F.avg("total_enrollment"), 2).alias("avg_enrollment"),
    )

    grid = (
        base.join(cell_neighbor_counts(schools), on="h3_cell", how="left")
        .join(cell_neighbor_distance(schools), on="h3_cell", how="left")
        .join(cell_nearest_distance(schools), on="h3_cell", how="left")
        .withColumn(
            "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
        )
        .transform(lambda d: dt.to_web_mercator(d, "geometry"))
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
