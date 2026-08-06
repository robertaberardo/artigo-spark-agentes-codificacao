"""Código que a execução acrescentou a utils/, extraído função a função.

Sintético: serve só de corpo de análise estática. Origem:
  - dataframe_transforms.py:count_h3_kring_members
  - dataframe_transforms.py:h3_kring_neighbor_distances
  - dataframe_transforms.py:nearest_neighbor_distance
"""

def count_h3_kring_members(
    df: DataFrame,
    cell_col: str,
    k: int = 1,
    out_col: str = "n_kring_members",
    exact_ring: bool = False,
) -> DataFrame:
    """Conta, por célula H3, quantas linhas caem na sua vizinhança de raio ``k``.

    Para cada célula distinta de ``cell_col``, soma os membros das células no
    k-ring H3 (com ``exact_ring=False`` inclui a própria célula e todas as
    adjacentes até ``k``). Devolve ``cell_col`` e ``out_col`` — o tamanho da
    vizinhança, idêntico para todas as linhas de uma mesma célula. Requer
    ``SedonaContext`` ativa.
    """
    members = df.groupBy(cell_col).agg(F.count(F.lit(1)).alias("_kring_members"))
    ring = members.select(
        F.col(cell_col).alias("_kring_home"),
        F.explode(
            stf.ST_H3KRing(F.col(cell_col), F.lit(k), F.lit(exact_ring))
        ).alias("_kring_cell"),
    )
    return (
        ring.join(members, ring["_kring_cell"] == members[cell_col], "left")
        .groupBy("_kring_home")
        .agg(F.sum("_kring_members").alias(out_col))
        .select(F.col("_kring_home").alias(cell_col), F.col(out_col))
    )

def h3_kring_neighbor_distances(
    df: DataFrame,
    id_col: str,
    cell_col: str,
    geom_col: str = "geometry",
    k: int = 1,
    out_col: str = "dist_neighbors_m",
) -> DataFrame:
    """Distância geodésica média de cada linha às suas vizinhas no k-ring H3.

    São vizinhas as demais linhas cuja célula (``cell_col``) está no k-ring da
    célula da linha (própria célula + adjacentes, com ``k=1``). A média usa
    ``ST_DistanceSpheroid`` (geodésica, em metros), pelo que ``geom_col`` deve
    estar em WGS84. Devolve ``id_col``, ``cell_col`` e ``out_col``; linhas sem
    vizinha não aparecem. Requer ``SedonaContext`` ativa.
    """
    left = df.select(
        F.col(id_col).alias("_nbr_a_id"),
        F.col(cell_col).alias("_nbr_home"),
        F.col(geom_col).alias("_nbr_a_geom"),
    ).withColumn(
        "_nbr_ring",
        F.explode(stf.ST_H3KRing(F.col("_nbr_home"), F.lit(k), F.lit(False))),
    )
    right = df.select(
        F.col(id_col).alias("_nbr_b_id"),
        F.col(cell_col).alias("_nbr_b_cell"),
        F.col(geom_col).alias("_nbr_b_geom"),
    )
    other = F.col("_nbr_a_id") != F.col("_nbr_b_id")
    pairs = left.join(
        right, F.col("_nbr_ring") == F.col("_nbr_b_cell"), "inner"
    ).filter(other)
    distance = stf.ST_DistanceSpheroid(F.col("_nbr_a_geom"), F.col("_nbr_b_geom"))
    return (
        pairs.withColumn("_nbr_dist", distance)
        .groupBy("_nbr_a_id", "_nbr_home")
        .agg(F.avg("_nbr_dist").alias(out_col))
        .select(
            F.col("_nbr_a_id").alias(id_col),
            F.col("_nbr_home").alias(cell_col),
            F.col(out_col),
        )
    )

def nearest_neighbor_distance(
    df: DataFrame,
    id_col: str,
    geom_col: str = "geometry",
    out_col: str = "dist_nearest_m",
) -> DataFrame:
    """Distância geodésica de cada linha à linha mais próxima do conjunto inteiro.

    Faz um cross-join do conjunto consigo mesmo (adequado a conjuntos pequenos),
    mede com ``ST_DistanceSpheroid`` (em metros, sobre WGS84) e fica com a menor
    por linha. Devolve ``id_col`` e ``out_col``. Requer ``SedonaContext`` ativa.
    """
    left = df.select(
        F.col(id_col).alias("_nn_a_id"), F.col(geom_col).alias("_nn_a_geom")
    )
    right = df.select(
        F.col(id_col).alias("_nn_b_id"), F.col(geom_col).alias("_nn_b_geom")
    )
    pairs = left.crossJoin(right).filter(F.col("_nn_a_id") != F.col("_nn_b_id"))
    distance = stf.ST_DistanceSpheroid(F.col("_nn_a_geom"), F.col("_nn_b_geom"))
    return (
        pairs.withColumn("_nn_dist", distance)
        .groupBy("_nn_a_id")
        .agg(F.min("_nn_dist").alias(out_col))
        .select(F.col("_nn_a_id").alias(id_col), F.col(out_col))
    )
