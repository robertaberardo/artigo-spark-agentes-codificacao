"""Medida 2 · 2.1 — Aderência a schema / anti-alucinação.

Verifica que os **campos-fonte** (colunas do raw de `escola`/`matricula`)
referenciados pelo código gerado **existem** no schema real (fonte única
`evaluation/schema_source.py`). Não pontua colunas **criadas** pelo próprio
código (alias/withColumn/renomeações) — só o que é lido das fontes.

Como distingue leitura-de-fonte de coluna-derivada: os campos do Censo são
MAIÚSCULOS (`CO_ENTIDADE`, `QT_MAT_BAS`); as colunas derivadas seguem o house
style `snake_case` minúsculo. Logo, uma referência MAIÚSCULA que não existe em
nenhuma fonte e não foi criada no arquivo = alucinação. Referências minúsculas
que casam (case-insensitive) com um campo-fonte também contam como leitura
válida; as demais minúsculas são derivadas e ignoradas.

Formas de referência capturadas: `F.col("X")`, `col("X")`, `df["X"]`,
strings posicionais em `select/selectExpr? ` não — só nomes de coluna:
`select("X", ...)`, `groupBy("X")`, `orderBy("X")`, `drop("X")`, `agg`, e o
primeiro arg de `withColumn`/segundo de `withColumnRenamed` entram como
**criadas** (excluídas), não como leitura.

Uso:
    python schema_fields.py <candidato.py> [<candidato2.py> ...]
Saída: linhas por item (2.1.1 escola, 2.1.2 matricula) PASS/FAIL + evidência,
e `RATIO 2.1 = k/2`. Código de saída = nº de itens que falharam.
"""
from __future__ import annotations

import ast
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import schema_source as S  # noqa: E402

UPPER_FIELD = re.compile(r"^[A-Z][A-Z0-9_]*$")
# métodos cujos args string são NOMES DE COLUNA LIDA (referência a coluna existente)
READ_COL_METHODS = {"select", "groupBy", "groupby", "orderBy", "sort", "drop", "dropDuplicates"}
# métodos que CRIAM coluna (o nome não é leitura de fonte)
CREATE_METHODS = {"withColumn", "withColumnRenamed"}


def _str(node) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def collect(candidate: str) -> tuple[set[str], set[str]]:
    """Devolve (referenciadas, criadas) — nomes de coluna string no arquivo."""
    tree = ast.parse(open(candidate, encoding="utf-8").read())
    referenced: set[str] = set()
    created: set[str] = set()
    for node in ast.walk(tree):
        # alias("X") — cria X
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            attr = node.func.attr
            if attr == "alias":
                for a in node.args:
                    s = _str(a)
                    if s:
                        created.add(s)
            elif attr in CREATE_METHODS and node.args:
                if attr == "withColumn":
                    s = _str(node.args[0])
                    if s:
                        created.add(s)
                elif attr == "withColumnRenamed" and len(node.args) >= 2:
                    old, new = _str(node.args[0]), _str(node.args[1])
                    if old:
                        referenced.add(old)  # o "de" é leitura
                    if new:
                        created.add(new)
            elif attr == "col":  # F.col("X") / col("X")
                for a in node.args:
                    s = _str(a)
                    if s:
                        referenced.add(s)
            elif attr in READ_COL_METHODS:
                for a in node.args:
                    s = _str(a)
                    if s:
                        referenced.add(s)
        # col("X") como Name (import direto de col)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "col":
            for a in node.args:
                s = _str(a)
                if s:
                    referenced.add(s)
        # df["X"]
        elif isinstance(node, ast.Subscript):
            s = _str(node.slice)
            if s:
                referenced.add(s)
    return referenced, created


def evaluate(candidate: str) -> list[tuple[str, str, bool, str]]:
    """Devolve [(id, rótulo, passou, evidência)] para 2.1.1 e 2.1.2."""
    referenced, created = collect(candidate)
    valid_union = S.source_field_names()  # UPPER
    valid_upper = {v.upper() for v in valid_union}

    # candidatas a campo-fonte: referências em estilo MAIÚSCULO do Censo
    upper_refs = {r for r in referenced if UPPER_FIELD.match(r)}
    hallucinated = sorted(r for r in upper_refs if r.upper() not in valid_upper and r not in created)

    results = []
    for item, table in (("2.1.1", "escola"), ("2.1.2", "matricula")):
        tfields = {f.upper() for f in S.source_field_names(table)}
        read_here = sorted(r for r in upper_refs if r.upper() in tfields)
        passed = not hallucinated
        ev = f"lidos={read_here} alucinados={hallucinated}"
        results.append((item, table, passed, ev))
    return results


def main() -> int:
    caminhos = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not caminhos:
        print(__doc__)
        return 0

    # Combinação sobre o CORPO INTEIRO: um item é FAIL se houver campo alucinado em
    # QUALQUER arquivo. Emitir um bloco por arquivo faria o montador do checklist
    # descartar todos menos o primeiro, em silêncio.
    combinado: dict[str, list] = {}
    ordem: list[str] = []
    for path in caminhos:
        base = os.path.basename(path)
        try:
            results = evaluate(path)
        except (OSError, SyntaxError) as e:
            print(f"# ILEGÍVEL {base}: {e}")
            continue
        for item, table, passed, ev in results:
            if item not in combinado:
                combinado[item] = [table, passed, f"{base}: {ev}"]
                ordem.append(item)
            elif combinado[item][1] and not passed:
                combinado[item][1] = False
                combinado[item][2] = f"{base}: {ev}"

    print(f"# corpo: {', '.join(os.path.basename(c) for c in caminhos)}")
    n_pass = 0
    for item in ordem:
        table, passed, ev = combinado[item]
        tag = "PASS" if passed else "FAIL"
        n_pass += passed
        print(f"{item} {table} {tag}  {ev}")
    print(f"RATIO 2.1 = {n_pass}/2")
    return len(ordem) - n_pass


if __name__ == "__main__":
    sys.exit(main())
