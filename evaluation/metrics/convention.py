"""Medida 2 · 2.4 — Convenção / medallion (aderência inferida).

Lê os artefatos EXPORTADOS da run (árvore espelhando o bucket + código gerado +
`metadata.toml` da run) e verifica, sem instruir o agente:

  2.4.1  o job que produz a tabela-alvo está na camada certa (`.../gold/...`);
  2.4.2  a tabela foi salva no path da convenção `app/tabela/nível`;
  2.4.3  a tabela nova está declarada no `metadata.toml` (com o nível gold);
  2.4.4  a gold geoespacial usa o writer padrão `geoparquet`.

Uso:
    python convention.py --jobs-dir <apps/educacao/jobs> \
        --outputs <arvore_bucket_exportada> --metadata <metadata.toml> \
        [--app educacao] [--table educacao_escola_matricula_h3_grid] [--level gold]
Saída: linha por item (2.4.k PASS/FAIL) + `RATIO 2.4 = k/4`.
Código de saída = nº de itens que falharam.
"""
from __future__ import annotations

import os
import sys
import tomllib

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import schema_source as S  # noqa: E402


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


def find_producer(jobs_dir: str, table: str) -> str | None:
    """Arquivo .py que menciona o nome da tabela-alvo (o produtor da gold)."""
    for root, _dirs, files in os.walk(jobs_dir):
        for f in files:
            if f.endswith(".py"):
                p = os.path.join(root, f)
                try:
                    if table in open(p, encoding="utf-8").read():
                        return p
                except OSError:
                    pass
    return None


def path_in_tree(outputs: str, *parts: str) -> bool:
    """Existe um diretório que contenha, em sequência, os componentes dados?"""
    target = os.path.join(*parts).replace("\\", "/").lower()
    for root, dirs, _files in os.walk(outputs):
        norm = root.replace("\\", "/").lower()
        if target in norm:
            return True
    return False


def metadata_declares(meta_path: str, table: str, level: str) -> bool:
    try:
        meta = tomllib.load(open(meta_path, "rb"))
    except (OSError, tomllib.TOMLDecodeError):
        return False
    for _k, entry in meta.get("tables", {}).items():
        if entry.get("name") == table and level in entry.get("levels", []):
            return True
    return False


def evaluate(jobs_dir, outputs, meta_path, app, table, level, registrada=False):
    producer = find_producer(jobs_dir, table) if jobs_dir else None
    results = []

    # 2.4.1 job na camada certa
    in_gold = bool(producer) and f"{os.sep}{level}{os.sep}" in producer.replace("/", os.sep)
    results.append(("2.4.1", in_gold,
                    f"produtor={producer}" if producer else "produtor não encontrado"))

    # 2.4.2 salvo no path da convenção app/tabela/nível
    saved = bool(outputs) and path_in_tree(outputs, app, table, level)
    results.append(("2.4.2", saved, f"procurou {app}/{table}/{level} na árvore exportada"))

    # 2.4.3 declarada no metadata (nível gold)
    declared = bool(meta_path) and metadata_declares(meta_path, table, level)
    results.append(("2.4.3", declared, f"metadata={meta_path}"))

    # 2.4.4 geoparquet no writer da gold
    geo = bool(producer) and "geoparquet" in open(producer, encoding="utf-8").read()
    results.append(("2.4.4", geo, "writer geoparquet na gold" if geo else "sem geoparquet no produtor"))

    # 2.4.5 pipeline materializa bronze -> silver -> gold no lake.
    # Item definido APÓS a observação dos resultados (rotulado como tal no artigo):
    # os itens 2.4.1-2.4.3 já se propunham a medir a aderência ao medallion, mas não
    # detectavam a execução que salta as camadas intermediárias e grava só a final.
    # A mudança é conservadora: só pode reduzir pontuação, nunca elevá-la.
    # Na composição REGISTRADA este item não existia: o score a recomputa para
    # gerar o crosswalk sem depender de ler os checklists antigos.
    if not registrada:
        niveis = niveis_materializados(outputs, app) if outputs else set()
        completo = {"bronze", "silver", level} <= niveis
        results.append(("2.4.5", completo,
                        f"camadas materializadas em {app}/: {sorted(niveis) or 'nenhuma'}"))

    return results


def niveis_materializados(outputs: str, app: str) -> set[str]:
    """Nomes de camada presentes na árvore exportada, sob o app.

    Procura diretórios chamados bronze/silver/gold em qualquer profundidade abaixo do
    app, o que cobre tanto `app/tabela/nivel` quanto variações de organização.
    """
    niveis = set()
    raiz = os.path.join(outputs, app)
    if not os.path.isdir(raiz):
        raiz = outputs
    for base, dirs, _arquivos in os.walk(raiz):
        for d in dirs:
            if d in ("bronze", "silver", "gold"):
                niveis.add(d)
    return niveis


def main() -> int:
    jobs_dir = arg("--jobs-dir")
    outputs = arg("--outputs")
    meta_path = arg("--metadata")
    app = arg("--app", S.APP)
    table = arg("--table", S.TARGET_TABLE)
    level = arg("--level", S.TARGET_LEVEL)
    if not any([jobs_dir, outputs, meta_path]):
        print(__doc__)
        return 0

    results = evaluate(jobs_dir, outputs, meta_path, app, table, level,
                       registrada="--registrada" in sys.argv)
    n_pass = 0
    for item, passed, ev in results:
        tag = "PASS" if passed else "FAIL"
        n_pass += passed
        print(f"{item} {tag}  {ev}")
    print(f"RATIO 2.4 = {n_pass}/{len(results)}")
    return len(results) - n_pass


if __name__ == "__main__":
    sys.exit(main())
