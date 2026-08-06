"""Transformações de coluna específicas do domínio IBGE.
"""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def is_valid_municipio_code(col: Column) -> Column:
    """Valida o dígito verificador do código IBGE de município (7 dígitos).

    O código IBGE é composto por 6 dígitos de base (UF + sequencial) e um
    dígito verificador. O DV é uma soma ponderada dos seis primeiros dígitos
    com pesos alternados 1 e 2 (produtos de dois algarismos têm 9 subtraído),
    fechada em ``(10 - soma % 10) % 10``. Códigos malformados (fora do padrão
    de 7 dígitos), nulos ou com DV incorreto resultam em ``False`` — útil para
    barrar códigos fabricados que passariam por um filtro de formato.
    """
    digits = [F.substring(col, i + 1, 1).cast("int") for i in range(7)]
    products = [d * w for d, w in zip(digits[:6], [1, 2, 1, 2, 1, 2])]
    adjusted = [F.when(p >= 10, p - 9).otherwise(p) for p in products]
    total = adjusted[0]
    for term in adjusted[1:]:
        total = total + term
    check_digit = (F.lit(10) - (total % 10)) % 10
    well_formed = col.rlike(r"^[0-9]{7}$")
    return F.when(
        well_formed & (digits[6] == check_digit), F.lit(True)
    ).otherwise(F.lit(False))
