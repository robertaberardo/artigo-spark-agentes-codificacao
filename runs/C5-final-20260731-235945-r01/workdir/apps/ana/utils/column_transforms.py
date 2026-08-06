"""Transformações de coluna específicas do domínio ANA (recursos hídricos)."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def is_valid_station_code(col: Column) -> Column:
    """Indica se o texto é um código de estação fluviométrica bem formado.

    A ANA identifica cada estação por um código numérico de oito dígitos. Aqui
    exige-se exatamente oito dígitos após remover separadores; nulos e formatos
    diferentes resultam em ``False``.
    """
    digits = F.regexp_replace(F.trim(col), r"[^0-9]", "")
    return digits.rlike(r"^[0-9]{8}$")
