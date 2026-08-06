"""Transformações de coluna específicas do domínio educação (Censo Escolar)."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

# Códigos do Censo Escolar tal como aterrissam no raw (coluna SETOR = dependência
# administrativa; SITUACAO = situação de funcionamento).
SETOR_FEDERAL = "1"      # 1=federal, 2=estadual, 3=municipal, 4=privada
SITUACAO_ATIVA = "1"     # 1=em atividade, 2=paralisada, 3=extinta


def is_valid_school_id(col: Column) -> Column:
    """Indica se o texto é um ID_UNIDADE bem formado (só dígitos, não vazio).

    Cada unidade escolar é identificada por um código numérico; nulos e valores
    com caracteres não numéricos resultam em ``False``.
    """
    return F.trim(col).rlike(r"^[0-9]+$")


def is_federal(col: Column) -> Column:
    """Indica se a dependência administrativa (``setor``) é federal.

    No Censo Escolar o setor federal reúne os Institutos Federais, CEFETs,
    colégios de aplicação e escolas técnicas vinculadas à União.
    """
    return F.trim(col) == SETOR_FEDERAL


def is_active(col: Column) -> Column:
    """Indica se a situação de funcionamento (``situacao``) é "em atividade".

    Descarta escolas paralisadas ou extintas, mantendo apenas as em operação.
    """
    return F.trim(col) == SITUACAO_ATIVA
