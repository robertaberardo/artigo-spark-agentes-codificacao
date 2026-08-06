"""Medida 2 · 2.3 — Aderência a padrões de estilo (`CODING_STANDARDS.md`).

Avalia as **16 regras** confirmadas no doc (§6b, 2.3.1–2.3.16), emitindo
**PASS/FAIL por regra** + evidência (`Lnn`), para o `checklist.tsv`.

Cada regra é `strict` (detecção AST determinística) ou `heur` (heurística — a
regra é semântica; PASS/FAIL indicativo). Os alvos da validação §5b
(`withColumnRenamed` 2.3.4, `F.expr` 2.3.6, `inferSchema` 2.3.5) são `strict`.
As `heur` são coerentes com o D-2 (estilo pode operar como controle/efeito-teto).

Uso:  python style_check.py <arquivo.py> [...]
Saída: por arquivo, uma linha por regra + `RATIO 2.3 = k/16`.
Código de saída = nº de regras que falharam (somado nos arquivos).
"""
from __future__ import annotations

import ast
import os
import re
import sys

# atributos de DataFrame/Column legítimos (não são acesso-coluna por atributo)
DF_ATTRS = {
    "select", "selectExpr", "filter", "where", "groupBy", "groupby", "agg", "join",
    "withColumn", "withColumnRenamed", "drop", "dropDuplicates", "distinct", "orderBy",
    "sort", "alias", "cast", "write", "read", "na", "fillna", "dropna", "schema",
    "columns", "dtypes", "rdd", "count", "show", "collect", "limit", "union", "unionByName",
    "withColumnsRenamed", "transform", "repartition", "coalesce", "cache", "persist",
    "sparkContext", "createDataFrame", "toDF", "printSchema", "first", "head", "take",
    "isNotNull", "isNull", "desc", "asc", "over", "between", "otherwise", "when",
    "format", "option", "options", "load", "save", "mode", "parquet", "csv", "json",
    "builder", "appName", "getOrCreate", "config", "master", "stop", "setLogLevel",
}
CAMEL = re.compile(r"[a-z][a-zA-Z0-9]*[A-Z]")
SNAKE_OK = re.compile(r"^[a-z_][a-z0-9_]*$")
NON_ASCII_IDENT = re.compile(r"[^\x00-\x7F]")


def _txt(node) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _calls(tree):
    return [n for n in ast.walk(tree) if isinstance(n, ast.Call)]


def _attr_name(func) -> str | None:
    return func.attr if isinstance(func, ast.Attribute) else (func.id if isinstance(func, ast.Name) else None)


def check_file(path: str):
    src = open(path, encoding="utf-8").read()
    tree = ast.parse(src, filename=path)
    calls = _calls(tree)
    src_l = src.lower()
    rules: list[tuple[str, str, bool, str]] = []  # (id, kind, passed, evidence)

    def add(rid, kind, passed, ev=""):
        rules.append((rid, kind, passed, ev))

    # 2.3.1 sem import *
    v = [n.lineno for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)
         and any(a.name == "*" for a in n.names)]
    add("2.3.1", "strict", not v, f"import* em L{v}" if v else "sem import*")

    # 2.3.2 toda função com docstring
    nodoc = [n.name for n in ast.walk(tree)
             if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and ast.get_docstring(n) is None]
    add("2.3.2", "strict", not nodoc, f"sem docstring: {nodoc}" if nodoc else "todas com docstring")

    # 2.3.3 sem bare except
    v = [n.lineno for n in ast.walk(tree) if isinstance(n, ast.ExceptHandler) and n.type is None]
    add("2.3.3", "strict", not v, f"bare except em L{v}" if v else "sem bare except")

    # 2.3.4 sem withColumnRenamed
    v = [c.lineno for c in calls if _attr_name(c.func) == "withColumnRenamed"]
    add("2.3.4", "strict", not v, f"withColumnRenamed em L{v}" if v else "ausente")

    # 2.3.5 schema explícito (sem inferSchema)
    has_infer = "inferschema" in src_l
    add("2.3.5", "strict", not has_infer, "inferSchema presente" if has_infer else "sem inferSchema")

    # 2.3.6 F.* em vez de F.expr / selectExpr
    v = [c.lineno for c in calls if _attr_name(c.func) in {"expr", "selectExpr"}]
    add("2.3.6", "strict", not v, f"expr/selectExpr em L{v}" if v else "sem F.expr")

    # 2.3.7 sem UDF
    udf_refs = [n.lineno for n in ast.walk(tree)
                if (isinstance(n, ast.Attribute) and n.attr in {"udf", "pandas_udf"})
                or (isinstance(n, ast.Name) and n.id in {"udf", "pandas_udf"})]
    add("2.3.7", "strict", not udf_refs, f"udf em L{udf_refs}" if udf_refs else "sem UDF")

    # 2.3.8 join com how explícito e sem right
    bad_join = []
    for c in calls:
        if _attr_name(c.func) == "join":
            hows = [k for k in c.keywords if k.arg == "how"]
            if not hows:
                bad_join.append((c.lineno, "sem how"))
            else:
                val = _txt(hows[0].value)
                if val and val.lower() in {"right", "right_outer", "rightouter"}:
                    bad_join.append((c.lineno, "how=right"))
    add("2.3.8", "strict", not bad_join, f"join {bad_join}" if bad_join else "join(s) how explícito, sem right")

    # 2.3.9 sem dropDuplicates/distinct como muleta
    v = [c.lineno for c in calls if _attr_name(c.func) in {"dropDuplicates", "distinct"}]
    add("2.3.9", "strict", not v, f"dropDuplicates/distinct em L{v}" if v else "ausente")

    # 2.3.10 coluna via F.col()/string, não por atributo (df.coluna)
    attr_col = []
    for n in ast.walk(tree):
        if isinstance(n, ast.Call):  # marca os .func para não confundir método com coluna
            continue
    call_funcs = {id(c.func) for c in calls}
    for n in ast.walk(tree):
        if isinstance(n, ast.Attribute) and id(n) not in call_funcs:
            if isinstance(n.value, ast.Name) and SNAKE_OK.match(n.attr) and n.attr not in DF_ATTRS \
                    and n.value.id not in {"F", "T", "ct", "dt", "stf", "stc", "os", "sys", "spark"}:
                attr_col.append((n.lineno, f"{n.value.id}.{n.attr}"))
    add("2.3.10", "heur", not attr_col, f"acesso por atributo {attr_col[:5]}" if attr_col else "sem acesso-coluna por atributo")

    # 2.3.11 snake_case em nomes de função/variável (constante MAIÚSCULA ok)
    camel = set()
    for n in ast.walk(tree):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and CAMEL.search(n.name):
            camel.add(n.name)
        if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store) and CAMEL.search(n.id):
            camel.add(n.id)
    add("2.3.11", "heur", not camel, f"camelCase: {sorted(camel)}" if camel else "snake_case ok")

    # 2.3.12 idioma: identificadores em ASCII (EN); acento => provável PT no código
    nonascii = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Name) and NON_ASCII_IDENT.search(n.id):
            nonascii.add(n.id)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and NON_ASCII_IDENT.search(n.name):
            nonascii.add(n.name)
    add("2.3.12", "heur", not nonascii, f"ident não-ASCII: {sorted(nonascii)}" if nonascii else "identificadores ASCII")

    # 2.3.13 F.lit(None) para vazios (não preencher com ""/0)
    fill_bad = []
    for c in calls:
        name = _attr_name(c.func)
        if name == "lit" and c.args:
            s = _txt(c.args[0])
            if s == "":
                fill_bad.append((c.lineno, 'F.lit("")'))
        if name in {"fillna", "fill"} and c.args:
            a0 = c.args[0]
            if isinstance(a0, ast.Constant) and a0.value in (0, "", 0.0):
                fill_bad.append((c.lineno, f"fillna({a0.value!r})"))
    add("2.3.13", "heur", not fill_bad, f"preenche vazio {fill_bad}" if fill_bad else "sem preenchimento de vazio por ''/0")

    # 2.3.14 aliases de import (functions as F; types as T)
    ok_F = any(isinstance(n, ast.ImportFrom) and n.module and n.module.endswith("functions")
               and any(a.asname == "F" for a in n.names) for n in ast.walk(tree))
    imports_types = any(isinstance(n, ast.ImportFrom) and n.module and n.module.endswith("types")
                        for n in ast.walk(tree))
    ok_T = (not imports_types) or any(
        isinstance(n, ast.ImportFrom) and n.module and n.module.endswith("types")
        and any(a.asname == "T" for a in n.names) for n in ast.walk(tree))
    # se nem importa functions, não há o que aliasar → não penaliza
    imports_functions = any(isinstance(n, ast.ImportFrom) and n.module and n.module.endswith("functions")
                            for n in ast.walk(tree))
    passed_14 = (ok_F or not imports_functions) and ok_T
    add("2.3.14", "heur", passed_14, "aliases F/T" if passed_14 else "faltou alias F/T")

    # 2.3.15 lat/long em double (via to_coordinate ou cast double)
    refs = src_l
    mentions_latlong = ("latitude" in refs) or ("longitude" in refs)
    uses_coord = "to_coordinate" in refs
    casts_double = 'cast("double")' in refs or "cast('double')" in refs
    passed_15 = (not mentions_latlong) or uses_coord or casts_double
    add("2.3.15", "heur", passed_15, "lat/long double" if passed_15 else "lat/long sem double/to_coordinate")

    # 2.3.16 distância geodésica sobre WGS84 (ST_DistanceSpheroid, não ST_Distance projetado)
    uses_dist = "distance" in refs
    geodesic = "distancespheroid" in refs or "add_distance_to_reference" in refs
    plain_st_distance = bool(re.search(r"st_distance\s*\(", refs))
    passed_16 = (not uses_dist) or geodesic or (not plain_st_distance)
    add("2.3.16", "heur", passed_16, "geodésica WGS84" if passed_16 else "ST_Distance sem spheroid")

    return rules


def check_corpus(caminhos: list[str]):
    """Avalia as 16 regras sobre o CORPO INTEIRO, emitindo um único bloco.

    Regra de combinação: uma regra é **FAIL se falhar em qualquer arquivo** — violação
    em um lugar é violação. A evidência aponta o primeiro arquivo em que falhou.

    Emitir um bloco por arquivo seria um erro silencioso: o montador do checklist
    registra a primeira ocorrência de cada identificador e descarta as demais, de modo
    que os arquivos seguintes ao primeiro nunca seriam lidos.
    """
    combinado: dict[str, list] = {}
    ordem: list[str] = []
    for path in caminhos:
        base = os.path.basename(path)
        try:
            rules = check_file(path)
        except (OSError, SyntaxError) as e:
            print(f"# ILEGÍVEL {base}: {e}")
            continue
        for rid, kind, passed, ev in rules:
            if rid not in combinado:
                combinado[rid] = [kind, passed, f"{base}: {ev}"]
                ordem.append(rid)
            elif combinado[rid][1] and not passed:
                combinado[rid][1] = False
                combinado[rid][2] = f"{base}: {ev}"
    return [(rid, combinado[rid][0], combinado[rid][1], combinado[rid][2]) for rid in ordem]


def main() -> int:
    caminhos = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not caminhos:
        print(__doc__)
        return 0
    print(f"# corpo: {', '.join(os.path.basename(c) for c in caminhos)}")
    rules = check_corpus(caminhos)
    n_pass = 0
    for rid, kind, passed, ev in rules:
        tag = "PASS" if passed else "FAIL"
        n_pass += passed
        print(f"{rid} [{kind}] {tag}  {ev}")
    print(f"RATIO 2.3 = {n_pass}/{len(rules)}")
    return len(rules) - n_pass


if __name__ == "__main__":
    sys.exit(main())
