"""Código que a execução acrescentou a utils/, extraído função a função.

Sintético: serve só de corpo de análise estática. Origem:
  - dataframe_transforms.py:add_h3_kring
  - dataframe_transforms.py:add_nearest_neighbor_distance
"""

def add_h3_kring(
    df: DataFrame, cell_col: str = "h3_cell", k: int = 1, out_col: str = "h3_kring"
) -> DataFrame:
    """Anexa o array das células H3 até ``k`` anéis de distância, incluindo a central.

    Usa ``ST_H3KRing`` (Sedona) com ``exact_ring=False``: para ``k=1`` devolve a
    própria célula e as seis imediatamente adjacentes. Explodir esse array e
    recasar as feições pela célula é a forma nativa de montar vizinhança de grade.
    Requer ``SedonaContext`` ativa.
    """
    return df.withColumn(
        out_col, stf.ST_H3KRing(F.col(cell_col), F.lit(k), F.lit(False))
    )

def add_nearest_neighbor_distance(
    df: DataFrame,
    id_col: str,
    geom_col: str = "geometry",
    out_col: str = "dist_nearest_m",
) -> DataFrame:
    """Anexa a distância geodésica de cada feição à vizinha mais próxima do próprio DataFrame.

    Faz o produto cartesiano do DataFrame consigo mesmo, descarta o pareamento de
    cada feição com ela própria (por ``id_col``) e toma a menor distância. A medida
    é geodésica (``ST_DistanceSpheroid``, elipsoide WGS84), então ``geom_col`` deve
    estar em coordenadas WGS84 em graus — não numa projeção métrica. Feições sem
    outra feição no conjunto recebem ``NULL``. Requer ``SedonaContext`` ativa e
    ``id_col`` único.
    """
    others = df.select(
        F.col(id_col).alias("_nn_other_id"),
        F.col(geom_col).alias("_nn_other_geom"),
    )
    nearest = (
        df.select(F.col(id_col), F.col(geom_col))
        .crossJoin(others)
        .filter(F.col(id_col) != F.col("_nn_other_id"))
        .withColumn(
            "_nn_dist",
            stf.ST_DistanceSpheroid(F.col(geom_col), F.col("_nn_other_geom")),
        )
        .groupBy(id_col)
        .agg(F.min("_nn_dist").alias(out_col))
    )
    return df.join(nearest, on=id_col, how="left")
