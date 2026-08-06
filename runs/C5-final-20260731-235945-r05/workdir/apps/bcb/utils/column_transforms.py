"""Transformações de coluna específicas do domínio BCB (séries econômicas)."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def is_valid_currency_code(col: Column) -> Column:
    """Indica se o texto é um código de moeda ISO 4217 bem formado.

    O código ISO de moeda tem três letras maiúsculas (ex.: ``USD``, ``EUR``,
    ``GBP``). Aqui basta a forma de três letras; nulos e outros formatos
    resultam em ``False``.
    """
    return F.upper(F.trim(col)).rlike(r"^[A-Z]{3}$")


def normalize_currency(col: Column) -> Column:
    """Padroniza o código da moeda: remove espaços e passa a maiúsculas."""
    return F.upper(F.trim(col))
