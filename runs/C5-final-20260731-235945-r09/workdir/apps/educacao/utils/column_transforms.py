"""Transformações de coluna específicas do domínio educação (INEP/educação básica)."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

# Códigos do Censo Escolar usados nos filtros de domínio.
ADMIN_SECTOR_FEDERAL = 1  # SETOR: 1 federal, 2 estadual, 3 municipal, 4 privada
OPERATING_STATUS_ACTIVE = 1  # SITUACAO: 1 em atividade, 2 paralisada, 3 extinta


def is_federal(admin_sector: Column) -> Column:
    """Indica se a escola pertence à rede federal (``SETOR = 1``).

    Trabalha sobre o código já normalizado (inteiro) do setor administrativo;
    nulos resultam em ``False``.
    """
    return admin_sector == F.lit(ADMIN_SECTOR_FEDERAL)


def is_in_activity(operating_status: Column) -> Column:
    """Indica se a escola está em atividade (``SITUACAO = 1``).

    Trabalha sobre o código já normalizado (inteiro) da situação de
    funcionamento; nulos resultam em ``False``.
    """
    return operating_status == F.lit(OPERATING_STATUS_ACTIVE)
