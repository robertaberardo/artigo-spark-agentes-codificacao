from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

# Códigos da fonte (Censo Escolar): setor administrativo e situação de
# funcionamento. Extraídos para constantes para não deixar literais soltos nos
# filtros dos jobs.
SETOR_FEDERAL = "1"
SITUACAO_EM_ATIVIDADE = "1"


def is_federal(setor: Column) -> Column:
    """Indica se a escola é da rede federal (``SETOR`` = 1)."""
    return F.trim(setor) == SETOR_FEDERAL


def is_active(situacao: Column) -> Column:
    """Indica se a escola está em atividade (``SITUACAO`` = 1)."""
    return F.trim(situacao) == SITUACAO_EM_ATIVIDADE
