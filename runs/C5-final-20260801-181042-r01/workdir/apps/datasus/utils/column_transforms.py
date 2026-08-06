"""Transformações de coluna específicas do domínio DATASUS (saúde pública)."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def is_valid_cnes(col: Column) -> Column:
    """Indica se o texto é um código CNES de estabelecimento bem formado.

    O CNES (Cadastro Nacional de Estabelecimentos de Saúde) é um código numérico
    de sete dígitos. Aqui os dígitos são extraídos e a validação exige exatamente
    sete deles; nulos e formatos diferentes resultam em ``False``.
    """
    digits = F.regexp_replace(F.trim(col), r"[^0-9]", "")
    return digits.rlike(r"^[0-9]{7}$")


def is_valid_cid(col: Column) -> Column:
    """Indica se o texto segue a forma de um código CID-10.

    O CID-10 tem uma letra seguida de dois dígitos, com um subgrupo decimal
    opcional (ex.: ``I21`` ou ``I21.0``). A comparação ignora a caixa; nulos e
    formatos diferentes resultam em ``False``.
    """
    return F.upper(F.trim(col)).rlike(r"^[A-Z][0-9]{2}(\.[0-9])?$")
