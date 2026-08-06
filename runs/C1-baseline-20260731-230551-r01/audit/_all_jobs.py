# === /d/runs/C1-baseline-20260731-230551-r01/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
"""Grade H3 nível 5 das escolas federais em atividade, com matrículas e vizinhança.

Agrega as escolas federais (``SETOR == 1``) em atividade (``SITUACAO == 1``) e
georreferenciadas por célula H3 de resolução 5. Além da contagem de escolas, das
matrículas e da média por escola, calcula três métricas de vizinhança:

* ``n_federal_neighbors``: quantidade de escolas federais em atividade situadas na
  própria célula ou nas células imediatamente adjacentes (disco H3 k=1), menos a
  própria escola — valor idêntico para toda a célula.
* ``avg_dist_neighbors_km``: média, por célula, da distância média de cada escola
  às suas vizinhas (as demais escolas do disco k=1).
* ``avg_dist_nearest_km``: média, por célula, da distância de cada escola à escola
  federal em atividade mais próxima, sem restrição de célula.

As distâncias são geodésicas (``ST_DistanceSpheroid``, elipsoide WGS84) em km.
"""

from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_functions as stf

from utils import column_transforms as ct
from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "raw")
MATRICULA = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
SETOR_FEDERAL = "1"
SITUACAO_ATIVA = "1"


def main():
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # --- matrículas: total da educação básica por escola ---------------------
    matricula = (
        spark.read.option("header", True).option("sep", ";").csv(MATRICULA)
    ).select(
        F.trim(F.col("ID_UNIDADE")).alias("id_unidade"),
        ct.coalesce_zero(F.col("MAT_BASICA").cast("long")).alias("enrollment"),
    )

    # --- escolas federais em atividade, georreferenciadas --------------------
    escola_raw = spark.read.option("header", True).option("sep", ";").csv(ESCOLA)
    escola = (
        escola_raw.filter(
            (F.trim(F.col("SETOR")) == SETOR_FEDERAL)
            & (F.trim(F.col("SITUACAO")) == SITUACAO_ATIVA)
        )
        .select(
            F.trim(F.col("ID_UNIDADE")).alias("id_unidade"),
            ct.to_coordinate(F.col("LATITUDE")).alias("latitude"),
            ct.to_coordinate(F.col("LONGITUDE")).alias("longitude"),
        )
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude", "geom"))
        .transform(lambda d: dt.attach_h3_index(d, "geom", H3_LEVEL, "h3_cell"))
    )

    schools = (
        escola.join(matricula, on="id_unidade", how="left")
        .withColumn("enrollment", ct.coalesce_zero(F.col("enrollment")))
        .select("id_unidade", "enrollment", "geom", "h3_cell")
    ).cache()

    # --- base: contagem, matrículas e média por célula -----------------------
    base = schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("enrollment").cast("long").alias("total_enrollment"),
    ).withColumn(
        "avg_enrollment",
        F.round(F.col("total_enrollment") / F.col("n_schools"), 2),
    )

    # --- n_federal_neighbors: escolas no disco H3 k=1, menos a própria -------
    # Vale para toda a célula: (nº de escolas na célula + adjacentes) - 1.
    cell_cnt = schools.groupBy("h3_cell").agg(F.count(F.lit(1)).alias("cnt"))
    ring = cell_cnt.select(
        F.col("h3_cell").alias("center_cell"),
        F.explode(stf.ST_H3KRing(F.col("h3_cell"), F.lit(1), F.lit(False))).alias(
            "ring_cell"
        ),
    )
    neighbors = (
        ring.join(
            cell_cnt.select(
                F.col("h3_cell").alias("ring_cell"), F.col("cnt").alias("ring_cnt")
            ),
            on="ring_cell",
            how="inner",
        )
        .groupBy("center_cell")
        .agg((F.sum("ring_cnt") - F.lit(1)).cast("long").alias("n_federal_neighbors"))
        .withColumnRenamed("center_cell", "h3_cell")
    )

    # --- avg_dist_neighbors_km: distância de cada escola às suas vizinhas -----
    s_rings = schools.select(
        F.col("id_unidade").alias("s_id"),
        F.col("geom").alias("s_geom"),
        F.col("h3_cell").alias("s_cell"),
    ).withColumn(
        "ring_cell",
        F.explode(stf.ST_H3KRing(F.col("s_cell"), F.lit(1), F.lit(False))),
    )
    neighbor_pairs = (
        s_rings.join(
            schools.select(
                F.col("id_unidade").alias("t_id"),
                F.col("geom").alias("t_geom"),
                F.col("h3_cell").alias("ring_cell"),
            ),
            on="ring_cell",
            how="inner",
        )
        .filter(F.col("s_id") != F.col("t_id"))
        .withColumn(
            "dist_km", stf.ST_DistanceSpheroid(F.col("s_geom"), F.col("t_geom")) / 1000.0
        )
    )
    dist_neighbors = (
        neighbor_pairs.groupBy("s_cell", "s_id")
        .agg(F.avg("dist_km").alias("school_mean_dist"))
        .groupBy("s_cell")
        .agg(F.round(F.avg("school_mean_dist"), 2).alias("avg_dist_neighbors_km"))
        .withColumnRenamed("s_cell", "h3_cell")
    )

    # --- avg_dist_nearest_km: distância à federal mais próxima (qualquer) -----
    nearest_pairs = (
        schools.select(
            F.col("id_unidade").alias("s_id"),
            F.col("geom").alias("s_geom"),
            F.col("h3_cell").alias("s_cell"),
        )
        .crossJoin(
            schools.select(
                F.col("id_unidade").alias("t_id"), F.col("geom").alias("t_geom")
            )
        )
        .filter(F.col("s_id") != F.col("t_id"))
        .withColumn(
            "dist_km", stf.ST_DistanceSpheroid(F.col("s_geom"), F.col("t_geom")) / 1000.0
        )
    )
    dist_nearest = (
        nearest_pairs.groupBy("s_cell", "s_id")
        .agg(F.min("dist_km").alias("school_nearest_dist"))
        .groupBy("s_cell")
        .agg(F.round(F.avg("school_nearest_dist"), 2).alias("avg_dist_nearest_km"))
        .withColumnRenamed("s_cell", "h3_cell")
    )

    # --- monta a grade e reconstrói a geometria da célula (EPSG:3857) --------
    grid = (
        base.join(neighbors, on="h3_cell", how="left")
        .join(dist_neighbors, on="h3_cell", how="left")
        .join(dist_nearest, on="h3_cell", how="left")
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

