"""Avaliação das execuções preservadas sob a rubrica da especificação.

Avalia cada execução de `runs/` e escreve em `evaluation/analysis/score/<run>/`:
as saídas dos instrumentos em `audit/` e o `checklist.tsv`, com 41 verificações
pontuadas e 3 variáveis registradas.

**Nenhuma sessão de agente é gerada** e **nada é escrito sob a raiz das execuções**,
que é somente leitura: há guarda explícita, não apenas convenção.

Corpo de análise estática = `audit/_all_jobs.py` **mais** os arquivos que a execução
promoveu para `workdir/utils/`, apurados por diff contra o baseline congelado. Quem
obedeceu ao documento de padrões e promoveu helpers saía do campo de visão do
instrumento registrado.

Uso:
    python score.py [--runs <raiz>] [--out <dir>] [--baseline <dir>]
"""
from __future__ import annotations

import glob
import io
import json
import os
import re
import subprocess
import sys

AQUI = os.path.dirname(os.path.abspath(__file__))
EXPERIMENT = os.path.dirname(AQUI)
RAIZ = os.path.dirname(EXPERIMENT)

sys.path.insert(0, AQUI)
import score_compare as rc  # noqa: E402

RUNS_PADRAO = os.environ.get("RUNS_ROOT", os.path.join(RAIZ, "runs"))
OUT_PADRAO = os.path.join(AQUI, "score")
BASELINE_PADRAO = os.path.join(RAIZ, "repo-baseline")
APP = "educacao"


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


def guarda_somente_leitura(destino: str, runs_root: str) -> None:
    """Aborta se o destino cair sob a raiz das execuções."""
    try:
        comum = os.path.commonpath([os.path.abspath(destino), os.path.abspath(runs_root)])
    except ValueError:
        return
    if comum == os.path.abspath(runs_root):
        raise SystemExit(f"!! RECUSADO: {destino} está sob {runs_root}, que é somente leitura.")


def _funcoes_de(caminho: str) -> dict[str, str]:
    """Funções de topo de um módulo: nome -> código-fonte."""
    import ast
    try:
        texto = open(caminho, encoding="utf-8").read()
        arvore = ast.parse(texto)
    except (OSError, SyntaxError):
        return {}
    return {n.name: ast.get_source_segment(texto, n) or ""
            for n in arvore.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}


def extrair_promovido(run_dir: str, baseline: str, destino: str) -> str | None:
    """Extrai o código que a execução ACRESCENTOU a `utils/`, e só ele.

    O corpo de análise tem de conter o código **do agente**, não o módulo de
    utilitários inteiro. `dataframe_transforms.py` *é* o módulo da casa: incluí-lo
    completo faria toda função utilitária aparecer como redefinida localmente, e
    cobraria do agente o estilo de código que ele não escreveu. Compara-se, portanto,
    função a função contra o baseline congelado, e mantêm-se apenas as **novas ou
    alteradas**.

    Devolve o caminho do módulo sintético, ou ``None`` se a execução nada promoveu.
    """
    wu = os.path.join(run_dir, "workdir", "utils")
    bu = os.path.join(baseline, "utils")
    if not os.path.isdir(wu):
        return None
    trechos, origem = [], []
    for nome in sorted(os.listdir(wu)):
        if not nome.endswith(".py") or nome.startswith("__"):
            continue
        atuais = _funcoes_de(os.path.join(wu, nome))
        originais = _funcoes_de(os.path.join(bu, nome)) if os.path.isfile(
            os.path.join(bu, nome)) else {}
        for fn, codigo in atuais.items():
            if originais.get(fn) != codigo:
                trechos.append(codigo)
                origem.append(f"{nome}:{fn}")
    if not trechos:
        return None
    caminho = os.path.join(destino, "_promovido_utils.py")
    with open(caminho, "w", encoding="utf-8", newline="\n") as fh:
        fh.write('"""Código que a execução acrescentou a utils/, extraído função a função.\n\n')
        fh.write("Sintético: serve só de corpo de análise estática. Origem:\n")
        for o in origem:
            fh.write(f"  - {o}\n")
        fh.write('"""\n\n')
        fh.write("\n\n".join(trechos) + "\n")
    return caminho


def _origens_promovidas(caminho: str) -> list[str]:
    """Lê do cabeçalho do módulo sintético quais funções foram promovidas."""
    return re.findall(r"^  - (\S+)$", open(caminho, encoding="utf-8").read(), re.M)


def _ratio(saida: str):
    """Extrai `k` e `n` de uma linha `RATIO x.y = k/n` da saída de um instrumento."""
    m = re.search(r"RATIO \S+ = (\d+)/(\d+)", saida)
    return [int(m.group(1)), int(m.group(2))] if m else [0, 0]


def roda(modulo: str, argumentos: list[str]) -> str:
    """Executa um instrumento e devolve sua saída, sem deixar o código de saída abortar."""
    p = subprocess.run([sys.executable, modulo] + argumentos,
                       capture_output=True, text=True, encoding="utf-8", errors="replace")
    return (p.stdout or "") + (p.stderr or "")


def uma_execucao(run_dir: str, ref: str, out_root: str, baseline: str) -> dict:
    run = os.path.basename(run_dir.rstrip("/\\"))
    destino = os.path.join(out_root, run)
    audit = os.path.join(destino, "audit")
    os.makedirs(audit, exist_ok=True)

    # --- 1. comparação numérica e geométrica, sob os três predicados ---------------
    r = rc.comparar(run_dir, ref)
    with open(os.path.join(audit, f"oracle_{rc.TABELA}.txt"), "w",
              encoding="utf-8", newline="\n") as fh:
        fh.write(rc.emitir_texto(r, "adaptativo"))

    # --- 2. corpo de análise estática ---------------------------------------------
    jobs = os.path.join(run_dir, "audit", "_all_jobs.py")
    promovido = extrair_promovido(run_dir, baseline, audit)
    corpo = ([jobs] if os.path.isfile(jobs) else []) + ([promovido] if promovido else [])
    utils_dir = os.path.join(run_dir, "workdir", "utils")

    if not corpo:
        # Caminho de instrumento ausente: os itens têm de aparecer como avaliados e
        # reprovados, para o gate de completude invalidar a execução em vez de produzir
        # um checklist com menos linhas que ninguém percebe.
        with open(os.path.join(audit, "metrics_absent.txt"), "w",
                  encoding="utf-8", newline="\n") as fh:
            fh.write("# corpo de análise ausente: sem _all_jobs.py e sem utils promovidos\n")
            for i in range(1, 3):
                fh.write(f"2.1.{i} - FAIL  corpo ausente\n")
            for i in range(1, 11):
                fh.write(f"2.2.{i} - FAIL  corpo ausente\n")
            for i in range(1, 17):
                fh.write(f"2.3.{i} - FAIL  corpo ausente\n")
            fh.write("VAR dedupe INDETERMINADO  corpo ausente  [registrado 2.2.6]\n")
    else:
        mdir = os.path.join(EXPERIMENT, "metrics")
        for nome, mod, extra in (
            ("schema_fields.txt", os.path.join(mdir, "schema_fields.py"), []),
            ("reuse_check.txt", os.path.join(mdir, "reuse_check.py"),
             ["--utils-dir", utils_dir]),
            ("style_check.txt", os.path.join(mdir, "style_check.py"), []),
        ):
            with open(os.path.join(audit, nome), "w", encoding="utf-8", newline="\n") as fh:
                fh.write(roda(mod, corpo + extra))

    # --- 3. convenção --------------------------------------------------------------
    saida_conv = roda(os.path.join(EXPERIMENT, "metrics", "convention.py"), [
        "--jobs-dir", os.path.join(run_dir, "workdir", "apps"),
        "--outputs", os.path.join(run_dir, "outputs"),
        "--metadata", os.path.join(run_dir, "workdir", "apps", APP, "metadata.toml"),
    ])
    with open(os.path.join(audit, "convention.txt"), "w", encoding="utf-8", newline="\n") as fh:
        fh.write(saida_conv)

    # --- 3b. composição REGISTRADA, recomputada -------------------------------------
    # Recomputa a leitura de aderência sob a composição de 34 itens a partir dos mesmos
    # artefatos: 12 funções de reúso uma a uma, 4 itens de convenção, e corpo restrito
    # ao job concatenado. É o que torna o crosswalk **regenerável** — sem isso, ele
    # dependeria de ler os checklists antigos, que serão descartados.
    m = os.path.join(EXPERIMENT, "metrics")
    corpo_reg = [jobs] if os.path.isfile(jobs) else []
    reg = {}
    if corpo_reg:
        for chave, mod, extra in (
            ("esquema", os.path.join(m, "schema_fields.py"), []),
            ("reuso", os.path.join(m, "reuse_check.py"),
             ["--utils-dir", utils_dir, "--registrada"]),
            ("estilo", os.path.join(m, "style_check.py"), []),
        ):
            reg[chave] = _ratio(roda(mod, corpo_reg + extra))
    reg["convencao"] = _ratio(roda(os.path.join(m, "convention.py"), [
        "--jobs-dir", os.path.join(run_dir, "workdir", "apps"),
        "--outputs", os.path.join(run_dir, "outputs"),
        "--metadata", os.path.join(run_dir, "workdir", "apps", APP, "metadata.toml"),
        "--registrada",
    ]))
    r["m2_registrada_recomputada"] = reg

    # --- 4. montagem do checklist --------------------------------------------------
    checklist = os.path.join(destino, "checklist.tsv")
    log = roda(os.path.join(EXPERIMENT, "run", "build_checklist.py"),
               ["--audit-dir", audit, "--out", checklist])
    with open(os.path.join(destino, "build.log"), "w", encoding="utf-8", newline="\n") as fh:
        fh.write(log)

    r["corpo"] = [os.path.basename(c) for c in corpo]
    r["utils_promovidos"] = _origens_promovidas(promovido) if promovido else []
    r["checklist"] = checklist
    r["completude_ok"] = "gate de completude: 41/41" in log
    return r


def main() -> int:
    runs_root = arg("--runs", RUNS_PADRAO)
    out_root = arg("--out", OUT_PADRAO)
    baseline = arg("--baseline", BASELINE_PADRAO)
    guarda_somente_leitura(out_root, runs_root)

    refs = sorted(glob.glob(os.path.join(EXPERIMENT, "gabaritos", rc.TABELA, "*.parquet")))
    if not refs:
        print("!! parquet da implementação de referência não encontrado")
        return 2
    runs = sorted(d for d in glob.glob(os.path.join(runs_root, "*")) if os.path.isdir(d))
    if not runs:
        print(f"!! nenhuma execução em {runs_root}")
        return 2

    os.makedirs(out_root, exist_ok=True)
    resultados = []
    for d in runs:
        r = uma_execucao(d, refs[0], out_root, baseline)
        resultados.append(r)
        marca = "ok " if r["completude_ok"] else "!! "
        promo = f" +utils:{','.join(r['utils_promovidos'])}" if r["utils_promovidos"] else ""
        print(f"{marca}{r['run']}{promo}")

    with open(os.path.join(out_root, "_comparacao.json"), "w",
              encoding="utf-8", newline="\n") as fh:
        json.dump(resultados, fh, ensure_ascii=False, indent=1)
    falhas = [r["run"] for r in resultados if not r["completude_ok"]]
    print(f"\n{len(resultados)} execuções reavaliadas; "
          f"gate de completude falhou em {len(falhas)}: {falhas or '—'}")
    return 1 if falhas else 0


if __name__ == "__main__":
    sys.exit(main())
