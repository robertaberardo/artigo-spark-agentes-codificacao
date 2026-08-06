"""Transformações de coluna específicas do domínio CAGED (emprego formal)."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def is_valid_cbo(col: Column) -> Column:
    """Indica se o texto é um código CBO de ocupação bem formado.

    O código CBO da Classificação Brasileira de Ocupações tem seis dígitos.
    Aqui basta a forma de seis dígitos; nulos e formatos diferentes resultam em
    ``False``.
    """
    return F.trim(col).rlike(r"^[0-9]{6}$")


def is_valid_cnae(col: Column) -> Column:
    """Indica se o texto é um código CNAE de atividade econômica bem formado.

    A subclasse CNAE 2.0 tem sete dígitos. Aqui basta a forma de sete dígitos;
    nulos e formatos diferentes resultam em ``False``.
    """
    return F.trim(col).rlike(r"^[0-9]{7}$")
