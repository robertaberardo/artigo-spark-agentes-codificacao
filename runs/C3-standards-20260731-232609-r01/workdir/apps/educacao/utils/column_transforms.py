"""Transformações de coluna específicas do domínio EDUCACAO (Censo Escolar)."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def is_valid_school_id(col: Column) -> Column:
    """Indica se o texto é um código INEP de escola bem formado.

    O código INEP da unidade escolar é um número de oito dígitos. Os dígitos são
    extraídos e a validação exige exatamente oito; nulos e formatos diferentes
    resultam em ``False``.
    """
    digits = F.regexp_replace(F.trim(col), r"[^0-9]", "")
    return digits.rlike(r"^[0-9]{8}$")
