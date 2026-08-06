"""Transformações de coluna específicas do domínio DNIT (rodovias federais)."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def is_valid_br(col: Column) -> Column:
    """Indica se o texto é uma sigla de BR bem formada.

    As rodovias federais são identificadas por um número de dois ou três
    dígitos (ex.: ``101``, ``40``). Aceita a forma ``BR-101`` ou apenas o
    número; nulos e formatos diferentes resultam em ``False``.
    """
    digits = F.regexp_replace(F.trim(col), r"[^0-9]", "")
    return digits.rlike(r"^[0-9]{2,3}$")


def normalize_br(col: Column) -> Column:
    """Padroniza a sigla de BR no número com três dígitos (ex.: ``40`` -> ``040``).

    Extrai os dígitos e completa com zeros à esquerda, para casar a mesma
    rodovia grafada de formas diferentes. Retorna ``NULL`` se não sobrar dígito.
    """
    digits = F.regexp_replace(F.trim(col), r"[^0-9]", "")
    return F.when(digits == "", None).otherwise(F.lpad(digits, 3, "0"))
