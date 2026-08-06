"""Transformações de coluna específicas do domínio TSE (eleições)."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def normalize_party_acronym(col: Column) -> Column:
    """Padroniza a sigla de um partido: remove espaços e coloca em maiúsculas."""
    return F.upper(F.trim(col))


def is_valid_party_acronym(col: Column) -> Column:
    """Indica se o texto é uma sigla de partido bem formada.

    A sigla de legenda é composta apenas por letras (após normalização, em
    maiúsculas), com dois a quinze caracteres — ex.: ``PT``, ``MDB``, ``PSDB``,
    ``REPUBLICANOS``. Nulos e formatos com dígitos ou símbolos resultam em
    ``False``.
    """
    return F.upper(F.trim(col)).rlike(r"^[A-Z]{2,15}$")
