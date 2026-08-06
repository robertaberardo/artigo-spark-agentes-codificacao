"""Código que a execução acrescentou a utils/, extraído função a função.

Sintético: serve só de corpo de análise estática. Origem:
  - dataframe_transforms.py:add_nearest_neighbor_distance
  - dataframe_transforms.py:add_h3_ring_neighbor_stats
"""

def add_nearest_neighbor_distance(
    df: DataFrame,
    id_col: str,
    point_col: str = "geometry",
    out_col: str = "nearest_dist_m",
) -> DataFrame:
    """Distância geodésica de cada linha à OUTRA linha mais próxima do conjunto.

    Faz um self cross join (produto cartesiano) e, por ``id_col`` (identificador
    único da linha), toma a menor ``ST_DistanceSpheroid`` até qualquer outra linha
    — a própria linha é descartada pela comparação de ``id_col``. ``point_col``
    deve estar em WGS84 (graus): a distância é medida sobre as coordenadas
    originais, não sobre uma projeção métrica. Linhas isoladas (conjunto de uma
    linha) ficam com ``out_col`` nulo. O produto cartesiano é O(n²) — próprio para
    conjuntos pequenos. Requer ``SedonaContext`` ativa.
    """
    other = df.select(
        F.col(id_col).alias("_nn_id"), F.col(point_col).alias("_nn_point")
    ).alias("other")
    distance = stf.ST_DistanceSpheroid(F.col(f"self.{point_col}"), F.col("other._nn_point"))
    nearest = (
        df.alias("self")
        .crossJoin(other)
        .filter(F.col(f"self.{id_col}") != F.col("other._nn_id"))
        .groupBy(F.col(f"self.{id_col}").alias(id_col))
        .agg(F.min(distance).alias(out_col))
    )
    return df.join(nearest, on=id_col, how="left")

def add_h3_ring_neighbor_stats(
    df: DataFrame,
    id_col: str,
    cell_col: str,
    point_col: str = "geometry",
    k: int = 1,
    count_out: str = "n_ring_neighbors",
    dist_out: str = "avg_ring_dist_m",
) -> DataFrame:
    """Vizinhança H3 de cada linha: quantas vizinhas e a distância média até elas.

    Vizinha é toda OUTRA linha cuja célula H3 (``cell_col``) esteja no disco de
    raio ``k`` em torno da célula da linha (``ST_H3KRing`` com ``exact_ring=False``,
    portanto a própria célula e as ``k`` camadas adjacentes). Faz um self cross
    join e agrega por ``id_col``: ``count_out`` conta as vizinhas e ``dist_out`` é
    a média das ``ST_DistanceSpheroid`` até elas. Como o disco depende só da célula,
    ``count_out`` é idêntico para todas as linhas de uma mesma célula. Linhas sem
    vizinha recebem contagem ``0`` (é uma contagem real, não ausência) e distância
    ``NULL``. ``point_col`` deve estar em WGS84 (a distância é medida sobre as
    coordenadas originais). Produto cartesiano O(n²) — próprio para conjuntos
    pequenos. Requer ``SedonaContext`` ativa.
    """
    other = df.select(
        F.col(id_col).alias("_rn_id"),
        F.col(cell_col).alias("_rn_cell"),
        F.col(point_col).alias("_rn_point"),
    ).alias("other")
    disk = stf.ST_H3KRing(F.col(f"self.{cell_col}"), F.lit(k), F.lit(False))
    is_neighbor = F.array_contains(disk, F.col("other._rn_cell"))
    not_self = F.col(f"self.{id_col}") != F.col("other._rn_id")
    distance = stf.ST_DistanceSpheroid(F.col(f"self.{point_col}"), F.col("other._rn_point"))
    stats = (
        df.alias("self")
        .crossJoin(other)
        .filter(not_self & is_neighbor)
        .groupBy(F.col(f"self.{id_col}").alias(id_col))
        .agg(
            F.count(F.lit(1)).cast("long").alias(count_out),
            F.avg(distance).alias(dist_out),
        )
    )
    joined = df.join(stats, on=id_col, how="left")
    return joined.withColumn(count_out, F.coalesce(F.col(count_out), F.lit(0)).cast("long"))
