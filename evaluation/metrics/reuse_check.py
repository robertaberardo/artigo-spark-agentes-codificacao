"""Medida 2 · 2.2 — Reúso das capacidades de ``utils/``, por classes de equivalência.

Cada uma das **dez classes** do mapa de reúso deve ser exercida por **chamada a
`utils/`**, não por reimplementação inline. Uma classe é PASS se **qualquer** de seus
membros for chamado; membro **redefinido localmente** não conta, porque é justamente a
reimplementação que a métrica precisa pegar.

Classes preservam o construto "reusou a capacidade existente" melhor que a contagem
função a função, que media escolha entre equivalentes: reprojetar via
``to_web_mercator`` em vez de ``reproject_geometry`` é substituição legítima, não
ausência de reúso. Ver `docs/ESPECIFICACAO.md §5.4`.

``deduplicate_by_key`` **não compõe o denominador**: o documento de padrões
desaconselha a operação e o dado congelado não tem duplicidade, de modo que não a
chamar é o comportamento conforme. Continua **reportada como variável registrada**.

**Corpo de análise.** Recebe um ou mais arquivos e os trata como **um único corpo**,
emitindo **um** bloco de itens. Isso é necessário porque o montador do checklist
registra a primeira ocorrência de cada identificador: emitir um bloco por arquivo
faria os arquivos seguintes serem descartados em silêncio.

Uso:
    python reuse_check.py <arquivo.py> [outro.py ...] [--utils-dir <dir>]
Saída: uma linha por classe (2.2.k PASS/FAIL), a variável de deduplicação, e
`RATIO 2.2 = k/10`.
Código de saída = nº de classes não exercidas.
"""
from __future__ import annotations

import ast
import os
import sys

# Mapa de reúso VIGENTE: dez classes de equivalência funcional.
# `registrado` = identificador(es) correspondente(s) na rubrica registrada, para o
# crosswalk ser gerado por código (os identificadores NÃO são estáveis entre as duas
# composições: a partir de 2.2.6 um mesmo número designa itens diferentes).
CLASSES = [
    ("blank_to_null",            ["blank_to_null"],                                    "2.2.1"),
    ("to_coordinate",            ["to_coordinate"],                                    "2.2.2"),
    ("normalize_text",           ["normalize_text"],                                   "2.2.3"),
    ("digits_only",              ["digits_only"],                                      "2.2.4"),
    ("zero_pad_code",            ["zero_pad_code"],                                    "2.2.5"),
    ("filter_valid_coordinates", ["filter_valid_coordinates"],                         "2.2.7"),
    ("join_no_fanout",           ["join_no_fanout"],                                   "2.2.8"),
    ("add_point_geometry",       ["add_point_geometry"],                               "2.2.9"),
    ("reprojecao",               ["to_web_mercator", "reproject_geometry"],            "2.2.10+2.2.11"),
    ("grade_h3",                 ["h3_join", "aggregate_by_h3", "attach_h3_index"],     "2.2.12"),
]
# Fora do denominador, reportada como variável registrada (§5.4 da especificação).
VARIAVEL_DEDUPE = "deduplicate_by_key"

# Composição REGISTRADA: as doze funções, uma a uma, como o instrumento avaliava no
# momento das execuções. Serve para o score recomputar aquela leitura a partir dos
# mesmos artefatos, de modo que o crosswalk seja **regenerável** e não dependa de ler
# os checklists antigos. Não é usada na avaliação vigente.
CLASSES_REGISTRADAS = [
    (fn, [fn], f"2.2.{i}") for i, fn in enumerate([
        "blank_to_null", "to_coordinate", "normalize_text", "digits_only",
        "zero_pad_code", "deduplicate_by_key", "filter_valid_coordinates",
        "join_no_fanout", "add_point_geometry", "to_web_mercator",
        "reproject_geometry", "h3_join",
    ], start=1)
]


def util_names(utils_dir: str) -> set[str]:
    names = set()
    if not os.path.isdir(utils_dir):
        return names
    for f in os.listdir(utils_dir):
        if f.endswith(".py") and not f.startswith("__"):
            try:
                tree = ast.parse(open(os.path.join(utils_dir, f), encoding="utf-8").read())
            except (OSError, SyntaxError):
                continue
            for node in tree.body:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    names.add(node.name)
    return names


def referenced_and_defined(caminhos: list[str]) -> tuple[dict[str, str], set[str]]:
    """Varre o corpo INTEIRO: devolve (referência -> 'arquivo:linha', definidas localmente).

    A combinação sobre o corpo é 'usada em qualquer arquivo', que é a semântica certa
    para reúso: chamar o util num módulo promovido continua sendo reúso.
    """
    refs: dict[str, str] = {}
    defined: set[str] = set()
    for caminho in caminhos:
        try:
            tree = ast.parse(open(caminho, encoding="utf-8").read())
        except (OSError, SyntaxError):
            continue
        base = os.path.basename(caminho)
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                defined.add(node.name)
            elif isinstance(node, ast.Attribute):
                refs.setdefault(node.attr, f"{base}:{node.lineno}")
            elif isinstance(node, ast.Name):
                refs.setdefault(node.id, f"{base}:{node.lineno}")
    return refs, defined


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


def evaluate(caminhos: list[str], utils_dir: str, classes=None):
    """Devolve (resultados, uso_dedupe). Resultado = (id, classe, passou, evidência, registrado)."""
    utils = util_names(utils_dir)
    refs, defined = referenced_and_defined(caminhos)
    classes = classes if classes is not None else CLASSES

    def usada(nome: str) -> str | None:
        if nome in utils and nome in refs and nome not in defined:
            return refs[nome]
        return None

    resultados = []
    for i, (classe, membros, registrado) in enumerate(classes, start=1):
        onde = next(((m, usada(m)) for m in membros if usada(m)), None)
        if onde:
            ev = f"{onde[0]} em {onde[1]}"
        else:
            ev = f"nenhum membro de {{{', '.join(membros)}}} chamado de utils/"
        resultados.append((f"2.2.{i}", classe, bool(onde), ev, registrado))
    return resultados, usada(VARIAVEL_DEDUPE)


def main() -> int:
    caminhos = [a for a in sys.argv[1:] if not a.startswith("--")]
    # remove o valor de --utils-dir da lista de arquivos
    if "--utils-dir" in sys.argv:
        v = arg("--utils-dir")
        caminhos = [c for c in caminhos if c != v]
    if not caminhos:
        print(__doc__)
        return 0
    utils_dir = arg("--utils-dir", "repo-baseline/utils")
    registrada = "--registrada" in sys.argv
    classes = CLASSES_REGISTRADAS if registrada else CLASSES

    resultados, dedupe = evaluate(caminhos, utils_dir, classes)
    print(f"# corpo: {', '.join(os.path.basename(c) for c in caminhos)}"
          + ("  [composição REGISTRADA — recomputada para o crosswalk]" if registrada else ""))
    n_pass = 0
    for item, classe, passou, ev, registrado in resultados:
        n_pass += passou
        print(f"{item} {classe} [{registrado}] {'PASS' if passou else 'FAIL'}  {ev}")
    if not registrada:
        print(f"VAR dedupe {'usado' if dedupe else 'nao_usado'}  "
              f"{dedupe or VARIAVEL_DEDUPE + ' ausente'}  [registrado 2.2.6]")
    print(f"RATIO 2.2 = {n_pass}/{len(resultados)}")
    return len(resultados) - n_pass


if __name__ == "__main__":
    sys.exit(main())
