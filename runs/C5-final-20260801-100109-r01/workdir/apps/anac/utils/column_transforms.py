"""Transformações de coluna específicas do domínio ANAC (aviação civil)."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def is_valid_oaci(col: Column) -> Column:
    """Indica se o texto é um código OACI de aeródromo bem formado.

    O código OACI de localidade tem quatro letras (no Brasil começa por ``SB``,
    ``SD``, ``SI``, ``SJ``, ``SN``, ``SS`` ou ``SW``). Aqui basta a forma de
    quatro letras maiúsculas; nulos e formatos diferentes resultam em ``False``.
    """
    return F.upper(F.trim(col)).rlike(r"^[A-Z]{4}$")
