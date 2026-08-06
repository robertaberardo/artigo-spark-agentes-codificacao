"""Transformações de coluna específicas do domínio CVM (fundos de investimento)."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def is_valid_cnpj(col: Column) -> Column:
    """Indica se o texto tem a forma de um CNPJ (14 dígitos).

    Considera apenas a quantidade de dígitos: extrai os algarismos e verifica se
    sobram exatamente 14. Não valida os dígitos verificadores — serve para
    descartar linhas vazias ou com identificador visivelmente truncado.
    """
    digits = F.regexp_replace(F.trim(col), r"[^0-9]", "")
    return F.length(digits) == 14


def format_cnpj(col: Column) -> Column:
    """Formata um CNPJ (14 dígitos) na máscara ``00.000.000/0000-00``.

    Extrai os dígitos e aplica a pontuação padrão; se não houver 14 dígitos,
    devolve ``NULL`` para não propagar um identificador malformado.
    """
    digits = F.regexp_replace(F.trim(col), r"[^0-9]", "")
    masked = F.regexp_replace(
        digits, r"^(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})$", r"$1.$2.$3/$4-$5"
    )
    return F.when(F.length(digits) == 14, masked).otherwise(F.lit(None))
