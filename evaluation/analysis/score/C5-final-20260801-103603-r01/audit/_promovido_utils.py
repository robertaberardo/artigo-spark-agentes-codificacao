"""Código que a execução acrescentou a utils/, extraído função a função.

Sintético: serve só de corpo de análise estática. Origem:
  - dataframe_transforms.py:add_nearest_other_distance_km
  - dataframe_transforms.py:add_h3_ring_neighbor_stats
"""

def add_nearest_other_distance_km(
    df: DataFrame,
    id_col: str,
    point_col: str = "geometry",
    out_col: str = "dist_nearest_km",
) -> DataFrame:
    """Distância geodésica (km) de cada linha ao ponto mais próximo do conjunto.

    Para cada linha calcula a menor distância até *outra* linha (excluindo ela
    mesma via ``id_col``), medida com ``ST_DistanceSpheroid`` sobre o elipsoide
    WGS84 — logo ``point_col`` deve ser um ponto em EPSG:4326 (lat/long, sem
    projetar). Faz um produto cartesiano com ``broadcast`` do lado direito, então
    é indicado para conjuntos pequenos (ex.: poucas centenas de pontos). Linhas
    sem nenhum outro ponto recebem ``NULL``. Requer ``SedonaContext`` ativa.
    """
    base = df.select(F.col(id_col).alias("_id"), F.col(point_col).alias("_geom"))
    other = df.select(F.col(id_col).alias("_oid"), F.col(point_col).alias("_ogeom"))
    dist_km = stf.ST_DistanceSpheroid(F.col("_geom"), F.col("_ogeom")) / F.lit(1000.0)
    nearest = (
        base.crossJoin(F.broadcast(other))
        .filter(F.col("_id") != F.col("_oid"))
        .groupBy("_id")
        .agg(F.min(dist_km).alias(out_col))
        .withColumnRenamed("_id", id_col)
    )
    return df.join(nearest, on=id_col, how="left")

def add_h3_ring_neighbor_stats(
    df: DataFrame,
    id_col: str,
    cell_col: str,
    point_col: str = "geometry",
    k: int = 1,
    count_col: str = "n_neighbors",
    dist_col: str = "dist_neighbors_km",
) -> DataFrame:
    """Estatísticas de vizinhança H3: contagem e distância média às vizinhas.

    São vizinhas de uma linha as *outras* linhas cuja célula H3 (``cell_col``)
    está dentro do anel de raio ``k`` da célula da linha — isto é, na própria
    célula ou em células adjacentes (``ST_H3KRing`` com ``exact_ring=False``,
    que inclui o centro). Acrescenta ``count_col`` (nº de vizinhas, ``0`` quando
    não há) e ``dist_col`` (distância geodésica média até as vizinhas, em km,
    ``NULL`` quando não há). A distância usa ``ST_DistanceSpheroid``, logo
    ``point_col`` deve ser um ponto em EPSG:4326. Requer ``SedonaContext`` ativa.
    """
    disk = (
        df.select(
            F.col(id_col).alias("_id"),
            F.col(point_col).alias("_geom"),
            stf.ST_H3KRing(F.col(cell_col), F.lit(k), F.lit(False)).alias("_disk"),
        )
        .select("_id", "_geom", F.explode("_disk").alias("_disk_cell"))
    )
    neigh = df.select(
        F.col(id_col).alias("_nid"),
        F.col(cell_col).alias("_ncell"),
        F.col(point_col).alias("_ngeom"),
    )
    dist_km = stf.ST_DistanceSpheroid(F.col("_geom"), F.col("_ngeom")) / F.lit(1000.0)
    stats = (
        disk.join(F.broadcast(neigh), F.col("_disk_cell") == F.col("_ncell"), "inner")
        .filter(F.col("_id") != F.col("_nid"))
        .groupBy("_id")
        .agg(
            F.count(F.lit(1)).cast("long").alias(count_col),
            F.avg(dist_km).alias(dist_col),
        )
        .withColumnRenamed("_id", id_col)
    )
    # 0 vizinhas é uma contagem real (não ausência); a distância média fica NULL.
    return df.join(stats, on=id_col, how="left").withColumn(
        count_col, F.coalesce(F.col(count_col), F.lit(0).cast("long"))
    )
