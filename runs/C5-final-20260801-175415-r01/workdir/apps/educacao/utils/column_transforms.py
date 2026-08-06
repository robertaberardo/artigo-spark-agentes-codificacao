"""Transformações de coluna específicas do domínio Educação (Censo Escolar)."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

# Códigos do Censo Escolar (ver metadata.toml).
SETOR_FEDERAL = "1"
SITUACAO_EM_ATIVIDADE = "1"


def is_federal(col: Column) -> Column:
    """Indica se a escola é da rede federal (``SETOR == 1``).

    Compara o código de setor administrativo já saneado; nulos resultam em
    ``False``.
    """
    return F.coalesce(F.trim(col) == F.lit(SETOR_FEDERAL), F.lit(False))


def is_active(col: Column) -> Column:
    """Indica se a escola está em atividade (``SITUACAO == 1``).

    Compara o código de situação de funcionamento já saneado; nulos resultam em
    ``False``.
    """
    return F.coalesce(F.trim(col) == F.lit(SITUACAO_EM_ATIVIDADE), F.lit(False))
