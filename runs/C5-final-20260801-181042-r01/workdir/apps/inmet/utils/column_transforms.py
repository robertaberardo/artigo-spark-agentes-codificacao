"""Transformações de coluna específicas do domínio INMET (meteorologia)."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def is_valid_wmo_code(col: Column) -> Column:
    """Indica se o texto é um código de estação bem formado (WMO/INMET).

    As estações automáticas do INMET usam código alfanumérico de quatro
    posições (ex.: ``A001``) e as convencionais um código OMM de cinco dígitos
    (ex.: ``83377``). Aqui basta a forma de quatro ou cinco caracteres
    maiúsculos/dígitos; nulos e formatos diferentes resultam em ``False``.
    """
    return F.upper(F.trim(col)).rlike(r"^[A-Z0-9]{4,5}$")


def celsius_is_plausible(col: Column) -> Column:
    """Indica se a temperatura em °C está numa faixa física plausível.

    Descarta leituras de sensor claramente defeituosas mantendo a faixa de
    superfície observada (-90 °C a 60 °C). Fora do intervalo (ou nulo) devolve
    ``False``, para que o chamador troque o valor por ``NULL``.
    """
    return col.between(-90.0, 60.0)
