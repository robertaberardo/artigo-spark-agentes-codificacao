"""Código que a execução acrescentou a utils/, extraído função a função.

Sintético: serve só de corpo de análise estática. Origem:
  - dataframe_transforms.py:add_kring_neighbor_stats
  - dataframe_transforms.py:add_nearest_neighbor_distance
"""

def add_kring_neighbor_stats(
    df: DataFrame,
    id_col: str,
    cell_col: str,
    geom_col: str = "geometry",
    k: int = 1,
    count_col: str = "n_neighbors",
    dist_col: str = "avg_dist_neighbors_m",
) -> DataFrame:
    """Conta os vizinhos de cada linha e a distância geodésica média até eles.

    Vizinhas são as **outras** linhas cuja célula H3 (``cell_col``) cai no k-anel
    de ``ST_H3KRing`` — que já inclui a própria célula — da célula da linha. Como o
    anel depende só da célula, ``count_col`` é idêntico para todas as linhas de uma
    mesma célula. A distância é geodésica (``ST_DistanceSpheroid``, WGS84), então
    ``geom_col`` deve ser um ponto em EPSG:4326. Linhas sem vizinho recebem
    ``count_col=0`` e ``dist_col`` ``NULL`` (ausência não é zero). ``id_col`` deve
    identificar a linha unicamente. Requer ``SedonaContext`` ativa.
    """
    left = df.select(
        F.col(id_col).alias("_nb_id"),
        F.col(geom_col).alias("_nb_geom"),
        F.explode(stf.ST_H3KRing(F.col(cell_col), F.lit(k), F.lit(False))).alias("_nb_ring"),
    )
    right = df.select(
        F.col(id_col).alias("_nb_id_r"),
        F.col(cell_col).alias("_nb_cell_r"),
        F.col(geom_col).alias("_nb_geom_r"),
    )
    pairs = (
        left.join(right, left["_nb_ring"] == right["_nb_cell_r"], "inner")
        .filter(F.col("_nb_id") != F.col("_nb_id_r"))
        .withColumn(
            "_nb_dist", stf.ST_DistanceSpheroid(F.col("_nb_geom"), F.col("_nb_geom_r"))
        )
    )
    stats = pairs.groupBy("_nb_id").agg(
        F.count(F.lit(1)).cast("long").alias(count_col),
        F.avg("_nb_dist").alias(dist_col),
    ).withColumnRenamed("_nb_id", id_col)

    return df.join(stats, on=id_col, how="left").withColumn(
        count_col, F.coalesce(F.col(count_col), F.lit(0)).cast("long")
    )

def add_nearest_neighbor_distance(
    df: DataFrame,
    id_col: str,
    geom_col: str = "geometry",
    out_col: str = "dist_nearest_m",
    broadcast: bool = True,
) -> DataFrame:
    """Distância geodésica de cada linha (ponto) à outra linha mais próxima.

    Produto cartesiano do conjunto consigo mesmo (excluindo a própria linha via
    ``id_col``), tomando a menor ``ST_DistanceSpheroid`` (WGS84) — ``geom_col`` deve
    ser um ponto em EPSG:4326. É a busca do vizinho mais próximo **sem** restrição
    espacial (qualquer linha do conjunto). Linhas sem par recebem ``out_col``
    ``NULL``. Use ``broadcast`` apenas quando o conjunto couber em memória. Requer
    ``SedonaContext`` ativa.
    """
    others = df.select(
        F.col(id_col).alias("_nn_id_r"),
        F.col(geom_col).alias("_nn_geom_r"),
    )
    if broadcast:
        others = F.broadcast(others)
    nearest = (
        df.crossJoin(others)
        .filter(F.col(id_col) != F.col("_nn_id_r"))
        .withColumn(
            "_nn_dist", stf.ST_DistanceSpheroid(F.col(geom_col), F.col("_nn_geom_r"))
        )
        .groupBy(id_col)
        .agg(F.min("_nn_dist").alias(out_col))
    )
    return df.join(nearest, on=id_col, how="left")
