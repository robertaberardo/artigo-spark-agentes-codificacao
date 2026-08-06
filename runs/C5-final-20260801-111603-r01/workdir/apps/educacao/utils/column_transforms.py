"""Transformações de coluna específicas do domínio educação (Censo Escolar)."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def is_valid_unidade_code(col: Column) -> Column:
    """Indica se o texto é um código de unidade escolar bem formado.

    O Censo Escolar identifica cada unidade por um código numérico de oito
    dígitos. Aqui os dígitos são extraídos e a validação exige exatamente oito
    deles; nulos e formatos diferentes resultam em ``False``.
    """
    digits = F.regexp_replace(F.trim(col), r"[^0-9]", "")
    return digits.rlike(r"^[0-9]{8}$")
