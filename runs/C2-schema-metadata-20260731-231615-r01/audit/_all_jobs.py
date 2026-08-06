# === /d/runs/C2-schema-metadata-20260731-231615-r01/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
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
SETOR_FEDERAL = "1"       # SETOR: 1 federal, 2 estadual, 3 municipal, 4 privada
SITUACAO_ATIVIDADE = "1"  # SITUACAO: 1 em atividade, 2 paralisada, 3 extinta


def _read_csv(spark, path):
    """Lê um CSV do raw (cabeçalho, separador ';')."""
    return spark.read.option("header", True).option("sep", ";").csv(path)


def main():
    """Grade H3 nível 5 das escolas federais em atividade.

    Agrega por célula H3 as escolas federais (SETOR=1) em atividade (SITUACAO=1)
    com coordenadas válidas: contagem, matrículas (soma/média), tamanho da
    vizinhança (célula + adjacentes) e distâncias médias (às vizinhas e à federal
    mais próxima). As operações H3 e as distâncias geodésicas (ST_DistanceSpheroid)
    trabalham em EPSG:4326, então os pontos são mantidos em graus lon/lat.
    """
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # --- escolas federais em atividade, georreferenciadas (EPSG:4326) --------
    escola_raw = _read_csv(spark, ESCOLA)
    escolas = (
        escola_raw.filter(
            (F.col("SETOR") == SETOR_FEDERAL)
            & (F.col("SITUACAO") == SITUACAO_ATIVIDADE)
        )
        .select(
            ct.zero_pad_code(F.col("ID_UNIDADE"), 8).alias("id_unidade"),
            ct.to_coordinate(F.col("LATITUDE")).alias("lat"),
            ct.to_coordinate(F.col("LONGITUDE")).alias("lon"),
        )
        .transform(lambda d: dt.filter_valid_coordinates(d, "lat", "lon"))
        .dropDuplicates(["id_unidade"])
    )
    escolas = dt.add_point_geometry(escolas, "lat", "lon", "geometry")

    # --- matrículas da educação básica por unidade ---------------------------
    matricula_raw = _read_csv(spark, MATRICULA)
    matriculas = (
        matricula_raw.select(
            ct.zero_pad_code(F.col("ID_UNIDADE"), 8).alias("id_unidade"),
            ct.coalesce_zero(F.col("MAT_BASICA").cast("long")).alias("matriculas"),
        )
        .groupBy("id_unidade")
        .agg(F.sum("matriculas").cast("long").alias("matriculas"))
    )

    escolas = (
        escolas.join(matriculas, on="id_unidade", how="left")
        .withColumn("matriculas", ct.coalesce_zero(F.col("matriculas")))
        .withColumn(
            "h3_cell",
            F.element_at(stf.ST_H3CellIDs(F.col("geometry"), H3_LEVEL, False), 1),
        )
    ).cache()

    # --- base por célula: contagem e matrículas ------------------------------
    per_cell = (
        escolas.groupBy("h3_cell")
        .agg(
            F.count(F.lit(1)).cast("long").alias("n_schools"),
            F.sum("matriculas").cast("long").alias("total_enrollment"),
        )
        .withColumn(
            "avg_enrollment",
            F.round(F.col("total_enrollment") / F.col("n_schools"), 2),
        )
    )

    # --- vizinhança: célula + células imediatamente adjacentes (k-ring 1) -----
    # Para cada célula com escolas, o k-ring (nível 1) traz a própria célula e as
    # adjacentes; associando as escolas por essas células obtém-se o conjunto de
    # vizinhas de cada escola da célula.
    cell_ring = (
        escolas.select("h3_cell")
        .distinct()
        .withColumn("ring", stf.ST_H3KRing(F.col("h3_cell"), F.lit(1), F.lit(False)))
        .select("h3_cell", F.explode("ring").alias("neighbor_cell"))
    )
    nbr_schools = escolas.select(
        F.col("h3_cell").alias("neighbor_cell"),
        F.col("id_unidade").alias("nbr_id"),
        F.col("geometry").alias("nbr_geom"),
    )
    # escolas presentes na vizinhança (célula + adjacentes) de cada célula focal
    neighborhood = cell_ring.join(nbr_schools, on="neighbor_cell", how="inner")

    # tamanho da vizinhança N (inclui as escolas da própria célula); o número de
    # vizinhas de cada escola é N - 1 (exclui a si mesma) — igual para a célula.
    n_neighbors = neighborhood.groupBy("h3_cell").agg(
        (F.count("nbr_id") - F.lit(1)).cast("long").alias("n_federal_neighbors")
    )

    # distância média de cada escola às suas vizinhas, agregada na célula. Como
    # toda escola da célula tem o mesmo número de vizinhas (N - 1), a média das
    # distâncias par-a-par equivale à média das médias por escola.
    focal = escolas.select(
        "h3_cell",
        F.col("id_unidade").alias("focal_id"),
        F.col("geometry").alias("focal_geom"),
    )
    neighbor_dist = (
        focal.join(neighborhood, on="h3_cell", how="inner")
        .filter(F.col("focal_id") != F.col("nbr_id"))
        .withColumn(
            "dist_km",
            stf.ST_DistanceSpheroid(F.col("focal_geom"), F.col("nbr_geom"))
            / F.lit(1000.0),
        )
        .groupBy("h3_cell")
        .agg(F.round(F.avg("dist_km"), 3).alias("avg_dist_neighbors_km"))
    )

    # --- distância à federal em atividade mais próxima (qualquer célula) ------
    # ~700 escolas federais → o produto cartesiano (self-join) é barato.
    a = escolas.select(
        F.col("id_unidade").alias("a_id"),
        "h3_cell",
        F.col("geometry").alias("a_geom"),
    )
    b = escolas.select(
        F.col("id_unidade").alias("b_id"),
        F.col("geometry").alias("b_geom"),
    )
    nearest = (
        a.crossJoin(b)
        .filter(F.col("a_id") != F.col("b_id"))
        .withColumn(
            "dist_km",
            stf.ST_DistanceSpheroid(F.col("a_geom"), F.col("b_geom")) / F.lit(1000.0),
        )
        .groupBy("h3_cell", "a_id")
        .agg(F.min("dist_km").alias("nearest_km"))
        .groupBy("h3_cell")
        .agg(F.round(F.avg("nearest_km"), 3).alias("avg_dist_nearest_km"))
    )

    # --- montagem final ------------------------------------------------------
    result = (
        per_cell.join(n_neighbors, on="h3_cell", how="left")
        .join(neighbor_dist, on="h3_cell", how="left")
        .join(nearest, on="h3_cell", how="left")
        .withColumn(
            "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
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

    result.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

