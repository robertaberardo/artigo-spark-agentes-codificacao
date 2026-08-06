from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5


def main():
    """Grade H3 (nível 5) das escolas federais em atividade e suas matrículas.

    Por célula: contagem de escolas, matrículas totais/médias, e três medidas de
    vizinhança calculadas escola a escola e depois agregadas na célula:

    - ``n_federal_neighbors``: nº de escolas vizinhas — as outras federais ativas
      na própria célula ou nas células imediatamente adjacentes (H3 k-ring 1).
      É idêntico para toda escola da célula, então basta o primeiro valor.
    - ``avg_dist_neighbors_km``: média, sobre as escolas da célula, da distância
      média de cada escola às suas vizinhas.
    - ``avg_dist_nearest_km``: média, sobre as escolas da célula, da distância de
      cada escola à federal mais próxima (qualquer, sem restrição de célula).

    As distâncias são geodésicas (``ST_DistanceSpheroid`` sobre o ponto WGS84
    original); o H3 opera em EPSG:4326 e a geometria da célula é reprojetada para
    EPSG:3857, o CRS planar do lake.
    """
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("geoparquet").load(ORIGEM)

    # Célula H3 (o H3 opera em EPSG:4326, então usamos o ponto lon/lat original) e
    # o disco k=1 (célula + adjacentes) que define a vizinhança de cada escola.
    escolas = (
        silver.withColumn("_pt4326", stc.ST_Point(F.col("longitude"), F.col("latitude")))
        .withColumn(
            "h3_cell", F.element_at(stf.ST_H3CellIDs(F.col("_pt4326"), H3_LEVEL, False), 1)
        )
        .withColumn("h3_disk", stf.ST_H3KRing(F.col("h3_cell"), F.lit(1), F.lit(False)))
        .select("id_unidade", "total_matriculas", "latitude", "longitude", "h3_cell", "h3_disk")
    ).cache()

    # Lado direito do produto cartesiano (colunas renomeadas p/ evitar ambiguidade).
    outras = escolas.select(
        F.col("id_unidade").alias("b_id"),
        F.col("latitude").alias("b_lat"),
        F.col("longitude").alias("b_lon"),
        F.col("h3_cell").alias("b_cell"),
    )

    dist_m = stf.ST_DistanceSpheroid(
        stc.ST_Point(F.col("longitude"), F.col("latitude")),
        stc.ST_Point(F.col("b_lon"), F.col("b_lat")),
    )
    is_other = F.col("id_unidade") != F.col("b_id")
    # Vizinha: outra escola cuja célula está no disco k=1 desta escola.
    is_neighbor = is_other & F.array_contains(F.col("h3_disk"), F.col("b_cell"))

    per_school = (
        escolas.crossJoin(outras)
        .withColumn("_dist_m", dist_m)
        .groupBy("id_unidade")
        .agg(
            F.first("h3_cell").alias("h3_cell"),
            F.first("total_matriculas").alias("total_matriculas"),
            F.min(F.when(is_other, F.col("_dist_m"))).alias("nearest_m"),
            F.avg(F.when(is_neighbor, F.col("_dist_m"))).alias("avg_neighbor_m"),
            F.sum(F.when(is_neighbor, F.lit(1)).otherwise(F.lit(0))).alias("n_neighbors"),
        )
    )

    grid = (
        per_school.groupBy("h3_cell")
        .agg(
            F.count(F.lit(1)).alias("n_schools"),
            F.sum("total_matriculas").alias("total_enrollment"),
            F.round(F.avg("total_matriculas"), 2).alias("avg_enrollment"),
            # idêntico para toda escola da célula -> primeiro valor basta
            F.first("n_neighbors").cast("long").alias("n_federal_neighbors"),
            F.round(F.avg("avg_neighbor_m") / F.lit(1000.0), 3).alias("avg_dist_neighbors_km"),
            F.round(F.avg("nearest_m") / F.lit(1000.0), 3).alias("avg_dist_nearest_km"),
        )
        .withColumn(
            "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
        )
        .transform(lambda d: dt.reproject_geometry(d, "geometry", "EPSG:4326", "EPSG:3857"))
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
