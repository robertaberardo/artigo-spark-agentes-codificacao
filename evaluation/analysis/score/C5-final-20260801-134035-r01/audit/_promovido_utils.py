"""Código que a execução acrescentou a utils/, extraído função a função.

Sintético: serve só de corpo de análise estática. Origem:
  - dataframe_transforms.py:h3_neighbor_distance_stats
  - dataframe_transforms.py:nearest_point_distance
"""

def h3_neighbor_distance_stats(
    df: DataFrame,
    id_col: str,
    cell_col: str,
    point_col: str,
    k: int = 1,
    count_col: str = "n_neighbors",
    distance_col: str = "avg_dist_neighbors_m",
) -> DataFrame:
    """Conta e mede a distância média de cada ponto às suas vizinhas por H3.

    São vizinhas de um ponto as *outras* linhas cuja célula (``cell_col``) cai no
    k-anel de sua célula (``ST_H3KRing`` com ``exact_ring=False`` — inclui a
    própria célula e os anéis de 1 a ``k``): com ``k=1``, a mesma célula ou uma
    imediatamente adjacente. Como o k-anel só depende da célula, a contagem de
    vizinhas é idêntica para todos os pontos de uma mesma célula. A distância é
    geodésica (``ST_DistanceSpheroid``) sobre os pontos WGS84 originais, em metros.

    Devolve uma linha por ``id_col`` com ``count_col`` (nº de vizinhas) e
    ``distance_col`` (distância média às vizinhas). Pontos sem vizinha *não*
    aparecem no retorno — a ausência deve ser tratada com ``left`` join pelo
    chamador (contagem ausente = 0). Requer ``SedonaContext`` ativa.
    """
    ring = df.select(
        F.col(id_col).alias("_nb_id_a"),
        F.col(point_col).alias("_nb_point_a"),
        F.explode(stf.ST_H3KRing(F.col(cell_col), k, False)).alias("_nb_ring_cell"),
    )
    other = df.select(
        F.col(id_col).alias("_nb_id_b"),
        F.col(cell_col).alias("_nb_cell_b"),
        F.col(point_col).alias("_nb_point_b"),
    )
    is_neighbor = (F.col("_nb_ring_cell") == F.col("_nb_cell_b")) & (
        F.col("_nb_id_a") != F.col("_nb_id_b")
    )
    pairs = ring.join(other, is_neighbor, "inner").withColumn(
        "_nb_dist_m",
        stf.ST_DistanceSpheroid(F.col("_nb_point_a"), F.col("_nb_point_b")),
    )
    return (
        pairs.groupBy("_nb_id_a")
        .agg(
            F.count(F.lit(1)).alias(count_col),
            F.avg("_nb_dist_m").alias(distance_col),
        )
        .select(F.col("_nb_id_a").alias(id_col), count_col, distance_col)
    )

def nearest_point_distance(
    df: DataFrame,
    id_col: str,
    point_col: str,
    distance_col: str = "dist_nearest_m",
) -> DataFrame:
    """Distância geodésica de cada ponto ao ponto mais próximo do conjunto.

    Faz o produto cartesiano do conjunto consigo mesmo (excluindo a própria linha)
    e toma, por ``id_col``, a menor ``ST_DistanceSpheroid`` (metros, sobre os
    pontos WGS84 originais). Sem restrição de vizinhança — considera todo o
    conjunto. O custo é O(n²); adequado apenas a conjuntos pequenos. Pontos de um
    conjunto de tamanho 1 não aparecem no retorno. Requer ``SedonaContext`` ativa.
    """
    left = df.select(
        F.col(id_col).alias("_np_id_a"), F.col(point_col).alias("_np_point_a")
    )
    right = df.select(
        F.col(id_col).alias("_np_id_b"), F.col(point_col).alias("_np_point_b")
    )
    pairs = (
        left.crossJoin(right)
        .filter(F.col("_np_id_a") != F.col("_np_id_b"))
        .withColumn(
            "_np_dist_m",
            stf.ST_DistanceSpheroid(F.col("_np_point_a"), F.col("_np_point_b")),
        )
    )
    return (
        pairs.groupBy("_np_id_a")
        .agg(F.min("_np_dist_m").alias(distance_col))
        .select(F.col("_np_id_a").alias(id_col), distance_col)
    )
