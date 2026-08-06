"""Extrator das variáveis comportamentais das cinco análises de mecanismo (E1 a E5).

Regenera **por código** os números da análise de mecanismos, de modo que sejam
reproduzíveis a partir dos artefatos preservados e não apenas auditáveis.
Ver `docs/ESPECIFICACAO.md §6.4`.

Cobre a **camada mecânica**. A camada interpretativa, com os juízos sobre a transcrição,
vive no caderno de codificação e não é extraível.

Saída: ``score/variaveis-comportamentais.csv`` e ``.json``, uma linha por execução.

⚠️ **Ressalva de instrumentação, obrigatória no artigo.** Leitura de arquivo por comando
de shell não aparece como caminho de arquivo no registro de ferramentas: aparece só o
comando. Este extrator interpreta comandos com a lista de padrões abaixo, o que é
heurístico e pode subcontar. Toda variável de "leu X" é, portanto, **cota inferior**.

`D:/runs` é somente leitura: este módulo apenas lê.

Uso:
    python extract_behavior.py [--runs <raiz>] [--out <dir>]
"""
from __future__ import annotations

import ast
import csv
import glob
import json
import os
import re
import sys

AQUI = os.path.dirname(os.path.abspath(__file__))
EXPERIMENT = os.path.dirname(AQUI)
sys.path.insert(0, os.path.join(EXPERIMENT, "audit"))
from reconcile import load_hooks  # noqa: E402

RUNS_PADRAO = os.environ.get("RUNS_ROOT", os.path.join(os.path.dirname(EXPERIMENT), "runs"))
OUT_PADRAO = os.path.join(AQUI, "score")

# --- lista de padrões VERSIONADA: é ela que o artigo cita -----------------------------
# Comandos de shell que constituem leitura de arquivo. Heurística declarada.
LEITURA_SHELL = re.compile(
    r"\b(cat|head|tail|less|more|bat|nl|sed\s+-n|awk|grep|rg|mc\s+(cat|head)|"
    r"docker\s+exec\s+\S+\s+(cat|head))\b", re.I)

# Artefatos de contexto cujo alcance interessa às análises.
ARTEFATOS = {
    "coding_standards": re.compile(r"CODING_STANDARDS\.md", re.I),
    "metadata_educacao": re.compile(r"apps[\\/]+educacao[\\/]+metadata\.toml", re.I),
    "metadata_outro_app": re.compile(r"apps[\\/]+(?!educacao)[a-z0-9_]+[\\/]+metadata\.toml", re.I),
    "utils_dataframe": re.compile(r"utils[\\/]+dataframe_transforms\.py", re.I),
    "utils_column": re.compile(r"utils[\\/]+column_transforms\.py", re.I),
    "gold_hexbin": re.compile(r"jobs[\\/]+gold[\\/]+\w*hexbin\w*\.py", re.I),
    "gold_geo": re.compile(r"jobs[\\/]+(gold|silver)[\\/]+\w*(geo|bacia|aerodromo)\w*\.py", re.I),
    "claude_md": re.compile(r"CLAUDE\.md", re.I),
    "raw_csv": re.compile(r"(raw[\\/]|\.csv\b)", re.I),
}

# Regras cuja presença no arquivo de contexto persistente interessa a E5.
REGRAS_CLAUDE_MD = {
    "menciona_3857": re.compile(r"\b3857\b"),
    "menciona_4326": re.compile(r"\b4326\b"),
    "menciona_geoparquet": re.compile(r"geoparquet", re.I),
    "menciona_medallion": re.compile(r"bronze|silver|gold", re.I),
    "menciona_utils": re.compile(r"\butils/", re.I),
    # Arredondamento DE MÉTRICA. O negativo à frente exclui "round-trip", que é a regra
    # de reprojeção e não tem nada a ver: sem ele, a variável marcava 25 de 31 onde o
    # correto é 0, e contradiria a evidência de que a precisão não tem fonte escrita.
    "menciona_arredondamento": re.compile(r"arredond|\bF\.round\b|\bround\s*\((?!\s*trip)", re.I),
    "menciona_metadata": re.compile(r"metadata\.toml", re.I),
    "menciona_idioma": re.compile(r"docstring|portugu", re.I),
}


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


def _alvos(chamada) -> list[str]:
    """Texto em que procurar artefatos: caminho declarado e/ou o comando executado."""
    ti = chamada.get("input") or {}
    if not isinstance(ti, dict):
        return []
    alvos = []
    for k in ("file_path", "path", "notebook_path"):
        if ti.get(k):
            alvos.append(str(ti[k]))
    if ti.get("pattern"):
        alvos.append(str(ti.get("pattern")))
    cmd = ti.get("command")
    if cmd and LEITURA_SHELL.search(str(cmd)):
        alvos.append(str(cmd))
    return alvos


def sequencia_de_leitura(chamadas) -> tuple[list[str], int | None]:
    """Ordem dos artefatos alcançados e o índice da primeira escrita de job."""
    seq, primeira_escrita = [], None
    for i, c in enumerate(chamadas):
        ti = c.get("input") or {}
        if c.get("tool") in ("Write", "Edit", "MultiEdit") and isinstance(ti, dict):
            alvo = str(ti.get("file_path", ""))
            if alvo.endswith(".py") and primeira_escrita is None:
                primeira_escrita = i
        for texto in _alvos(c):
            for nome, rx in ARTEFATOS.items():
                if rx.search(texto) and (not seq or seq[-1] != nome):
                    seq.append(nome)
    return seq, primeira_escrita


def alcancou(chamadas, ate: int | None = None) -> dict[str, bool]:
    vistos = {k: False for k in ARTEFATOS}
    for i, c in enumerate(chamadas):
        if ate is not None and i >= ate:
            break
        for texto in _alvos(c):
            for nome, rx in ARTEFATOS.items():
                if rx.search(texto):
                    vistos[nome] = True
    return vistos


def analise_estatica(caminho: str) -> dict:
    """E1 e E2 pelo código: uso de arredondamento e ordem de reprojeção."""
    fora = {"usou_round": False, "reprojetou_apos_reconstruir": False,
            "usou_kring": False, "autovalidacao_no_job": False}
    if not os.path.isfile(caminho):
        return fora
    try:
        texto = open(caminho, encoding="utf-8", errors="replace").read()
    except OSError:
        return fora
    fora["usou_round"] = bool(re.search(r"\bF\.round\s*\(|\bround\s*\(", texto))
    fora["usou_kring"] = bool(re.search(r"ST_H3KRing|h3_k?ring", texto, re.I))
    m_geom = re.search(r"ST_H3ToGeom", texto, re.I)
    if m_geom:
        depois = texto[m_geom.end():]
        fora["reprojetou_apos_reconstruir"] = bool(
            re.search(r"to_web_mercator|ST_Transform|reproject_geometry", depois, re.I))
    fora["autovalidacao_no_job"] = bool(re.search(r"\bassert\b|raise ValueError", texto))
    try:
        ast.parse(texto)
    except SyntaxError:
        pass
    return fora


def uma(run_dir: str) -> dict:
    run = os.path.basename(run_dir.rstrip("/\\"))
    hooks = os.path.join(run_dir, "audit", "hooks.jsonl")
    _prompts, chamadas = load_hooks(hooks)
    seq, primeira = sequencia_de_leitura(chamadas)
    antes = alcancou(chamadas, primeira)
    total = alcancou(chamadas)

    ferramentas = {}
    for c in chamadas:
        ferramentas[c.get("tool") or "?"] = ferramentas.get(c.get("tool") or "?", 0) + 1
    submits = sum(1 for c in chamadas
                  if c.get("tool") == "Bash"
                  and "spark-submit" in str((c.get("input") or {}).get("command", "")))
    verificacao = sum(1 for c in chamadas
                      if c.get("tool") in ("Write", "Edit")
                      and re.search(r"(verify|check|valida)\w*\.py",
                                    str((c.get("input") or {}).get("file_path", "")), re.I))

    # Recuperação de erro: a execução produziu rastro de exceção e mesmo assim
    # convergiu até persistir a tabela. Sub-métrica de confiabilidade (§4.7).
    erros = sum(1 for c in chamadas
                if re.search(r"Traceback|AnalysisException|Py4JJavaError|\bERROR\b",
                             str(c.get("response") or "")))

    est = analise_estatica(os.path.join(run_dir, "audit", "_all_jobs.py"))

    cmd_path = os.path.join(run_dir, "workdir", "CLAUDE.md")
    regras = {}
    if os.path.isfile(cmd_path):
        txt = open(cmd_path, encoding="utf-8", errors="replace").read()
        regras = {k: bool(rx.search(txt)) for k, rx in REGRAS_CLAUDE_MD.items()}
        regras["claude_md_bytes"] = len(txt.encode("utf-8"))
    else:
        regras = {k: None for k in REGRAS_CLAUDE_MD}
        regras["claude_md_bytes"] = 0

    return {
        "run": run, "condicao": run.split("-")[0],
        # E5 — rota de aproximação
        "e5_sequencia": " > ".join(seq[:12]),
        "e5_artefatos_ate_primeira_escrita": sum(1 for v in antes.values() if v),
        "e5_chamadas_ate_primeira_escrita": primeira if primeira is not None else -1,
        "e5_chamadas_total": len(chamadas),
        "e5_ferramentas": ";".join(f"{k}={v}" for k, v in sorted(ferramentas.items())),
        "e5_spark_submits": submits,
        **{f"e5_cmd_{k}": v for k, v in regras.items()},
        # E1 — mimetismo de arredondamento
        "e1_leu_gold_hexbin_antes": antes["gold_hexbin"],
        "e1_usou_round_no_job": est["usou_round"],
        # E2 — conflito de CRS
        "e2_leu_coding_standards": total["coding_standards"],
        "e2_leu_utils_dataframe": total["utils_dataframe"],
        "e2_leu_metadata_outro_app": total["metadata_outro_app"],
        "e2_leu_gold_geo": total["gold_geo"],
        "e2_reprojetou_apos_reconstruir": est["reprojetou_apos_reconstruir"],
        # E3 — autovalidação
        "e3_escreveu_script_verificacao": verificacao > 0,
        "e3_invariante_no_job": est["autovalidacao_no_job"],
        # sub-métricas de confiabilidade (§4.7), camada mecânica
        "conf_chamadas_com_erro": erros,
        "conf_houve_erro_de_execucao": erros > 0,
        # E4 — rotas de derivação do esquema
        "e4_leu_metadata_educacao": total["metadata_educacao"],
        "e4_leu_raw_antes_de_escrever": antes["raw_csv"],
        "e4_leu_raw_por_shell": any(
            LEITURA_SHELL.search(str((c.get("input") or {}).get("command", "")))
            and ARTEFATOS["raw_csv"].search(str((c.get("input") or {}).get("command", "")))
            for c in chamadas if isinstance(c.get("input"), dict)),
        "usou_kring": est["usou_kring"],
    }


def main() -> int:
    runs_root = arg("--runs", RUNS_PADRAO)
    out = arg("--out", OUT_PADRAO)
    os.makedirs(out, exist_ok=True)
    runs = sorted(d for d in glob.glob(os.path.join(runs_root, "*")) if os.path.isdir(d))
    linhas = [uma(d) for d in runs]

    with open(os.path.join(out, "variaveis-comportamentais.json"), "w",
              encoding="utf-8", newline="\n") as fh:
        json.dump(linhas, fh, ensure_ascii=False, indent=1)
    campos = list(linhas[0].keys())
    with open(os.path.join(out, "variaveis-comportamentais.csv"), "w",
              encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=campos, delimiter=";")
        w.writeheader()
        w.writerows(linhas)
    print(f"{len(linhas)} execuções · {len(campos)} variáveis -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
