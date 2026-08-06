from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")


def main():
    """Grade H3 (nível 5) das escolas federais em atividade.

    Para cada célula H3 calcula: nº de escolas, total e média de matrículas, o nº
    de escolas federais vizinhas (mesma célula + células adjacentes, exceto a
    própria — igual para toda a célula), a distância geodésica média de cada
    escola às suas vizinhas e a distância geodésica média de cada escola à federal
    mais próxima (qualquer, sem restrição de célula). Distâncias medidas com
    ``ST_DistanceSpheroid`` sobre os pontos WGS84 originais.
    """
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = spark.read.format("geoparquet").load(ORIGEM)

    # Ponto WGS84 (para a distância geodésica) e disco H3 k=1 da célula da escola
    # (célula própria + 6 adjacentes; ``exact_ring=False`` inclui o centro).
    prepared = schools.select(
        F.col("id_unidade"),
        F.col("h3_cell"),
        F.col("enrollment"),
        stc.ST_Point(F.col("longitude"), F.col("latitude")).alias("point_wgs84"),
        stf.ST_H3KRing(F.col("h3_cell"), F.lit(1), F.lit(False)).alias("h3_disk"),
    )

    # Base por célula: contagem e matrículas independem do cálculo de vizinhança.
    cell_base = prepared.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("enrollment").cast("long").alias("total_enrollment"),
    )

    # Pares ordenados de escolas distintas. O conjunto é pequeno (centenas de
    # escolas federais), então o produto cartesiano é barato. Uma escola ``b`` é
    # vizinha de ``a`` quando a célula de ``b`` está no disco H3 da célula de ``a``.
    left = prepared.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("point_wgs84").alias("a_point"),
        F.col("h3_disk").alias("a_disk"),
    )
    right = prepared.select(
        F.col("id_unidade").alias("b_id"),
        F.col("h3_cell").alias("b_cell"),
        F.col("point_wgs84").alias("b_point"),
    )
    pairs = (
        left.crossJoin(right)
        .filter(F.col("a_id") != F.col("b_id"))
        .withColumn(
            "dist_m", stf.ST_DistanceSpheroid(F.col("a_point"), F.col("b_point"))
        )
        .withColumn("is_neighbor", F.array_contains(F.col("a_disk"), F.col("b_cell")))
    )

    # Por escola: média das distâncias às vizinhas, nº de vizinhas (= N-1, constante
    # na célula) e distância à federal mais próxima (mínimo sobre todas as demais).
    per_school = pairs.groupBy("a_id", "a_cell").agg(
        F.avg(F.when(F.col("is_neighbor"), F.col("dist_m"))).alias("mean_dist_neighbors_m"),
        F.sum(F.when(F.col("is_neighbor"), F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("n_neighbors"),
        F.min("dist_m").alias("nearest_m"),
    )

    # Por célula: o nº de vizinhas é o mesmo para toda a célula (usa ``max``, que
    # coincide com qualquer escola); as distâncias são promediadas entre as escolas.
    cell_neighbor = per_school.groupBy("a_cell").agg(
        F.max("n_neighbors").alias("n_federal_neighbors"),
        F.avg("mean_dist_neighbors_m").alias("mean_dist_neighbors_m"),
        F.avg("nearest_m").alias("mean_dist_nearest_m"),
    ).withColumnRenamed("a_cell", "h3_cell")

    grid = (
        dt.join_no_fanout(cell_base, cell_neighbor, on="h3_cell", how="left")
        .withColumn(
            "avg_enrollment",
            F.round(F.col("total_enrollment") / F.col("n_schools"), 2),
        )
        .withColumn(
            "avg_dist_neighbors_km",
            F.round(F.col("mean_dist_neighbors_m") / F.lit(1000.0), 3),
        )
        .withColumn(
            "avg_dist_nearest_km",
            F.round(F.col("mean_dist_nearest_m") / F.lit(1000.0), 3),
        )
        # Polígono da célula (ST_H3ToGeom devolve em EPSG:4326) reprojetado para
        # EPSG:3857, o CRS de saída do lake.
        .withColumn(
            "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
        )
        .transform(dt.to_web_mercator)
    )

    result = grid.select(
        "h3_cell",
        "n_schools",
        "total_enrollment",
        "avg_enrollment",
        "n_federal_neighbors",
        "avg_dist_neighbors_km",
        "avg_dist_nearest_km",
        "geometry",
    )

    result.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
