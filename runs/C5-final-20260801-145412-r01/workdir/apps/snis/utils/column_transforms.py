"""Transformações de coluna específicas do domínio SNIS (saneamento básico)."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

# Pesos por abrangência do prestador (quanto maior o alcance, maior o peso).
_ABRANGENCIA_WEIGHTS = {
    "regional": 3,
    "microrregional": 2,
    "municipal": 1,
    "local": 1,
}


def is_valid_ibge_municipio(col: Column) -> Column:
    """Indica se o texto é um código IBGE de município bem formado.

    O código de município do IBGE tem sete dígitos (dois da UF mais cinco do
    município). Aqui basta a forma de sete dígitos; nulos e formatos diferentes
    resultam em ``False``.
    """
    return F.regexp_replace(F.trim(col), r"[^0-9]", "").rlike(r"^[0-9]{7}$")


def abrangencia_weight(col: Column) -> Column:
    """Converte a abrangência do prestador num peso numérico comparável.

    Alcances maiores (regional, microrregional) pesam mais que os locais, o que
    permite ordenar prestadores por amplitude de atuação. Valores não
    reconhecidos assumem o peso de um prestador local.
    """
    normalized = F.lower(F.trim(col))
    result = F.when(normalized == "regional", F.lit(_ABRANGENCIA_WEIGHTS["regional"]))
    result = result.when(
        normalized == "microrregional", F.lit(_ABRANGENCIA_WEIGHTS["microrregional"])
    )
    result = result.when(
        normalized == "municipal", F.lit(_ABRANGENCIA_WEIGHTS["municipal"])
    )
    return result.otherwise(F.lit(_ABRANGENCIA_WEIGHTS["local"]))
