from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
SETOR_FEDERAL = 1
SITUACAO_ATIVA = 1


def main():
    """Grade H3 (res 5) das escolas federais em atividade.

    Por célula: nº de escolas, total e média de matrículas, nº de escolas
    vizinhas (federais ativas na própria célula ou nas adjacentes — disco H3
    k=1 — exceto a própria escola; igual para toda a célula), distância média
    de cada escola às suas vizinhas e distância média de cada escola à federal
    ativa mais próxima (qualquer célula).

    Todas as distâncias são geodésicas (``ST_DistanceSpheroid``) medidas sobre o
    ponto WGS84 original; só a geometria da célula persiste em EPSG:3857.
    """
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("geoparquet").load(ORIGEM)

    # Federais em atividade, com ponto WGS84 original e célula H3 res 5.
    # (H3 opera em coordenadas geográficas; usamos o ponto 4326, não o 3857.)
    fed = (
        silver.filter(
            (F.col("setor") == SETOR_FEDERAL)
            & (F.col("situacao") == SITUACAO_ATIVA)
        )
        .withColumn(
            "point_wgs84", stc.ST_Point(F.col("longitude"), F.col("latitude"))
        )
        .withColumn(
            "h3_cell",
            F.element_at(stf.ST_H3CellIDs(F.col("point_wgs84"), H3_LEVEL, False), 1),
        )
        .select("id_unidade", "h3_cell", "total_enrollment", "point_wgs84")
    )
    # id_unidade é a chave; garante unicidade antes dos self-joins de distância.
    fed = dt.deduplicate_by_key(fed, ["id_unidade"]).cache()

    # --- Agregação básica por célula -----------------------------------------
    base = fed.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("total_enrollment").cast("long").alias("total_enrollment"),
        F.avg("total_enrollment").alias("avg_enrollment"),
    )

    # --- Vizinhas: federais ativas na própria célula ou nas adjacentes -------
    # Cada escola expande seu disco H3 k=1 (própria célula + 6 adjacentes) e
    # cruza com as demais escolas cuja célula caia nesse disco.
    source = fed.select(
        F.col("id_unidade").alias("sid"),
        F.col("h3_cell"),
        F.col("point_wgs84").alias("sgeom"),
    ).withColumn(
        "ring_cell",
        F.explode(stf.ST_H3KRing(F.col("h3_cell"), F.lit(1), F.lit(False))),
    )
    target = fed.select(
        F.col("id_unidade").alias("tid"),
        F.col("h3_cell").alias("tcell"),
        F.col("point_wgs84").alias("tgeom"),
    )
    pairs = (
        source.join(target, source["ring_cell"] == target["tcell"], "inner")
        .filter(F.col("sid") != F.col("tid"))
        .withColumn(
            "dist_km",
            stf.ST_DistanceSpheroid(F.col("sgeom"), F.col("tgeom")) / F.lit(1000.0),
        )
    )
    # Por escola: quantidade de vizinhas e distância média às vizinhas.
    per_school = pairs.groupBy("sid", "h3_cell").agg(
        F.count(F.lit(1)).alias("neighbor_count"),
        F.avg("dist_km").alias("mean_dist_neighbors_km"),
    )
    # Reincorpora escolas de células isoladas (sem vizinhas): count 0, dist NULL.
    per_school_full = fed.select(
        F.col("id_unidade").alias("sid"), F.col("h3_cell")
    ).join(per_school, ["sid", "h3_cell"], "left")

    neighbor_agg = per_school_full.groupBy("h3_cell").agg(
        # n_federal_neighbors = |N| - 1, idêntico para toda a célula.
        F.coalesce(F.max("neighbor_count"), F.lit(0)).cast("long").alias(
            "n_federal_neighbors"
        ),
        F.avg("mean_dist_neighbors_km").alias("avg_dist_neighbors_km"),
    )

    # --- Federal mais próxima (qualquer célula) ------------------------------
    left = fed.select(
        F.col("id_unidade").alias("aid"),
        F.col("h3_cell"),
        F.col("point_wgs84").alias("ageom"),
    )
    right = fed.select(
        F.col("id_unidade").alias("bid"),
        F.col("point_wgs84").alias("bgeom"),
    )
    nearest_per_school = (
        left.crossJoin(right)
        .filter(F.col("aid") != F.col("bid"))
        .withColumn(
            "dist_km",
            stf.ST_DistanceSpheroid(F.col("ageom"), F.col("bgeom")) / F.lit(1000.0),
        )
        .groupBy("aid", "h3_cell")
        .agg(F.min("dist_km").alias("nearest_km"))
    )
    nearest_agg = nearest_per_school.groupBy("h3_cell").agg(
        F.avg("nearest_km").alias("avg_dist_nearest_km")
    )

    # --- Consolidação por célula + geometria do hexágono (3857) --------------
    grid = (
        base.join(neighbor_agg, "h3_cell", "left")
        .join(nearest_agg, "h3_cell", "left")
        .withColumn(
            "geometry",
            stf.ST_Transform(
                F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1),
                F.lit("EPSG:4326"),
                F.lit("EPSG:3857"),
            ),
        )
        .select(
            "h3_cell",
            "n_schools",
            "total_enrollment",
            F.round("avg_enrollment", 2).alias("avg_enrollment"),
            "n_federal_neighbors",
            F.round("avg_dist_neighbors_km", 3).alias("avg_dist_neighbors_km"),
            F.round("avg_dist_nearest_km", 3).alias("avg_dist_nearest_km"),
            "geometry",
        )
    )

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
