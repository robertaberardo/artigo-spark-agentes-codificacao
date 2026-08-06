"""Código que a execução acrescentou a utils/, extraído função a função.

Sintético: serve só de corpo de análise estática. Origem:
  - dataframe_transforms.py:add_h3_kring
"""

def add_h3_kring(
    df: DataFrame, h3_col: str = "h3_cell", k: int = 1, out_col: str = "h3_kring"
) -> DataFrame:
    """Anexa o k-ring da célula H3: o disco de células vizinhas até distância ``k``.

    Usa ``ST_H3KRing`` (Sedona) com ``exactRing=False``, que devolve o disco
    **incluindo a própria célula central** — para ``k=1``, a célula e as 6
    adjacentes. A saída (``out_col``) é um ``array`` de ids de célula. Requer
    ``SedonaContext`` ativa.
    """
    return df.withColumn(
        out_col, stf.ST_H3KRing(F.col(h3_col), F.lit(k), F.lit(False))
    )
