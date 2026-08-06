"""Agrega as execuções sob as duas composições e emite as tabelas de resultado.

**Consome apenas saídas do score.** As duas leituras de aderência — a de 33 itens da
especificação e a de 34 da composição registrada — são ambas **recomputadas** pelo score
a partir dos mesmos artefatos preservados, de modo que nada aqui depende de arquivo
gravado no momento das execuções.

Escreve em ``<score>/``: ``por-execucao.tsv``, ``escada.tsv``, ``perfil-por-item.tsv``
e ``crosswalk.tsv``.

Uso:
    python aggregate.py [--score <dir>]
"""
from __future__ import annotations

import json
import math
import os
import statistics
import sys

Z = 1.96  # 95%

AQUI = os.path.dirname(os.path.abspath(__file__))
EXPERIMENT = os.path.dirname(AQUI)
SCORE_PADRAO = os.path.join(EXPERIMENT, "analysis", "score")

ITENS_M1 = [f"1.{i}" for i in range(1, 9)]
SUBDIM = {"esquema": ("2.1", 2), "reuso": ("2.2", 10), "estilo": ("2.3", 16),
          "convencao": ("2.4", 5)}

CAUSA_REGUA = "régua de comparação adaptativa"
CAUSA_CLASSE = "fusão em classe de equivalência"
CAUSA_CORPO = "corpo de análise estendido ao utils/ promovido"
CAUSA_NOVO = "item novo, sem contraparte registrada"

# Classes de reúso com mais de um membro (ver metrics/reuse_check.py::CLASSES).
CLASSES_MULTIMEMBRO = {"2.2.9", "2.2.10"}


def wilson(k: int, n: int, z: float = Z) -> tuple[float, float]:
    """IC de Wilson 95% para uma proporção binária (k sucessos em n)."""
    if n == 0:
        return (0.0, 0.0)
    p = k / n
    d = 1 + z * z / n
    centro = (p + z * z / (2 * n)) / d
    meia = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, centro - meia), min(1.0, centro + meia))


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


def ler_checklist(caminho: str) -> dict:
    """Lê um checklist de qualquer das duas séries: id -> (resultado, tipo, registrado)."""
    if not os.path.isfile(caminho):
        return {}
    linhas = open(caminho, encoding="utf-8").read().splitlines()
    if not linhas:
        return {}
    tem_tipo = "tipo" in linhas[0].split("\t")
    saida = {}
    for ln in linhas[1:]:
        c = ln.split("\t")
        if len(c) < 2:
            continue
        if tem_tipo:
            saida[c[0]] = (c[2] if len(c) > 2 else "", c[1], c[3] if len(c) > 3 else "")
        else:
            saida[c[0]] = (c[1], "pontuado", c[0])
    return saida


def passou(reg) -> bool:
    return bool(reg) and reg[0] == "PASS"


def crosswalk(linhas, por_run):
    """Uma linha por diferença entre as composições, com o efeito numérico apurado.

    As duas leituras são recomputadas pelo score sobre os mesmos artefatos, de modo
    que a tabela é **regenerável**: não depende dos checklists gravados no momento das
    execuções. O efeito é o que permite ao leitor calibrar, por número e não por prosa,
    se alguma diferença favorece o resultado reportado.
    """
    n = len(linhas)
    reuso = [x["m2_reuso"] for x in linhas]
    medallion = sum(1 for r in por_run.values()
                    if r.get("m2_registrada_recomputada") and not _tem_medallion(r))
    promoveram = [x["run"] for x in linhas if x["promoveu"]]
    delta = [x["m2_vigente"] - x["m2_registrada"] for x in linhas]
    return [
        ("fusão dos equivalentes de reprojeção em uma classe (2 itens -> 1)", "-1 item",
         f"reúso mediano {statistics.median(reuso):g}/10; deixa de punir escolha entre equivalentes"),
        ("deduplicação fora do denominador", "-1 item",
         "vira variável registrada; uso observado reportado à parte"),
        ("novo item de medallion 2.4.5", "+1 item",
         f"{medallion} de {n} execuções reprovam nele"),
        ("corpo de análise estendido ao utils/ promovido", "sem mudança",
         f"{len(promoveram)} execuções promoveram código e passam a ser avaliadas por ele"),
        ("EFEITO LÍQUIDO por execução", "34 -> 33",
         f"delta mediano {statistics.median(delta):+g}; faixa [{min(delta):+d}; {max(delta):+d}]"),
    ]


def _tem_medallion(r) -> bool:
    """Se a execução materializou as três camadas (item 2.4.5 vigente)."""
    import os as _os
    p = _os.path.join(SCORE_PADRAO, r["run"], "checklist.tsv")
    return passou(ler_checklist(p).get("2.4.5"))


def main() -> int:
    score = arg("--score", SCORE_PADRAO)
    comparacao = json.load(open(os.path.join(score, "_comparacao.json"), encoding="utf-8"))
    por_run = {r["run"]: r for r in comparacao}
    runs = sorted(por_run)
    vigentes = {run: ler_checklist(os.path.join(score, run, "checklist.tsv")) for run in runs}

    linhas = []
    for run in runs:
        novo = vigentes[run]
        r = por_run[run]
        promoveu = bool(r.get("utils_promovidos"))

        m1 = all(passou(novo.get(i)) for i in ITENS_M1)
        m2 = {k: sum(1 for i in novo if i.startswith(p + ".") and passou(novo[i]))
              for k, (p, _n) in SUBDIM.items()}
        # leitura sob a composição registrada, recomputada pelo score
        reg = r.get("m2_registrada_recomputada", {})
        m1_velho = r["composicoes"]["registrado"]["registrada_nove"]
        m2_velho = sum(v[0] for v in reg.values())

        linhas.append({"run": run, "condicao": run.split("-")[0], "m1_vigente": m1,
                       "m1_registrada": m1_velho, "m2_vigente": sum(m2.values()),
                       "m2_registrada": m2_velho, "crs": r.get("crs_candidato"),
                       "crs_ok": r.get("crs_ok"), "casas": r.get("casas", {}),
                       "promoveu": promoveu, **{f"m2_{k}": v for k, v in m2.items()}})

    with open(os.path.join(score, "por-execucao.tsv"), "w", encoding="utf-8",
              newline="\n") as fh:
        fh.write("run\tcondicao\tm1_vigente\tm1_registrada\tm2_vigente_33\tm2_registrada_34\t"
                 "esquema\treuso\testilo\tconvencao\tcrs\tcrs_conforme\tcasas\tpromoveu_utils\n")
        for x in linhas:
            casas = ";".join(f"{c}={d}" for c, d in sorted((x["casas"] or {}).items()))
            fh.write(f"{x['run']}\t{x['condicao']}\t{int(x['m1_vigente'])}\t"
                     f"{int(x['m1_registrada'])}\t{x['m2_vigente']}\t{x['m2_registrada']}\t"
                     f"{x['m2_esquema']}\t{x['m2_reuso']}\t{x['m2_estilo']}\t{x['m2_convencao']}\t"
                     f"{x['crs']}\t{int(bool(x['crs_ok']))}\t{casas}\t{int(x['promoveu'])}\n")

    c5 = [r for r in comparacao if r["run"].startswith("C5")]
    escada = [
        ("Como registrado (arredondamento duplo, 9 verificações)", "registrado", "registrada_nove"),
        ("Tolerância fixa de 0,005 (9 verificações)", "fixo", "registrada_nove"),
        ("Composição registrada — 9 verificações", "adaptativo", "registrada_nove"),
        ("M1 — correção funcional (8 verificações)", "adaptativo", "m1_oito"),
    ]
    with open(os.path.join(score, "escada.tsv"), "w", encoding="utf-8", newline="\n") as fh:
        fh.write("criterio\tc5_k\tc5_n\tc5_p\tic_lo\tic_hi\ttotal_k\ttotal_n\n")
        print(f"\n{'Critério':<54}{'C5':>9}{'IC 95%':>19}{'Total':>9}")
        for rot, pred, comp in escada:
            k5 = sum(1 for r in c5 if r["composicoes"][pred][comp])
            kt = sum(1 for r in comparacao if r["composicoes"][pred][comp])
            lo, hi = wilson(k5, len(c5))
            fh.write(f"{rot}\t{k5}\t{len(c5)}\t{k5/len(c5):.4f}\t{lo:.4f}\t{hi:.4f}\t{kt}\t"
                     f"{len(comparacao)}\n")
            print(f"{rot:<54}{k5:>4}/{len(c5)}  [{lo:.3f}; {hi:.3f}]{kt:>5}/{len(comparacao)}")

    ids = ITENS_M1 + [f"{p}.{j}" for _k, (p, n) in SUBDIM.items() for j in range(1, n + 1)]
    with open(os.path.join(score, "perfil-por-item.tsv"), "w", encoding="utf-8",
              newline="\n") as fh:
        fh.write("item\tk\tn\tic_lo\tic_hi\n")
        for iid in ids:
            k = sum(1 for run in runs if run.startswith("C5") and passou(vigentes[run].get(iid)))
            lo, hi = wilson(k, len(c5))
            fh.write(f"{iid}\t{k}\t{len(c5)}\t{lo:.4f}\t{hi:.4f}\n")

    # --- crosswalk: uma linha por diferença, com efeito numérico -------------------
    cw = crosswalk(linhas, por_run)
    with open(os.path.join(score, "crosswalk.tsv"), "w", encoding="utf-8",
              newline="\n") as fh:
        fh.write("diferenca\tefeito_denominador\tefeito_observado\n")
        for dif, den, efeito in cw:
            fh.write(f"{dif}\t{den}\t{efeito}\n")
    print("\n=== crosswalk: composição registrada (34) -> M2-r (33) ===")
    for dif, den, efeito in cw:
        print(f"  {dif:<50} {den:>9}   {efeito}")

    vig = sorted(x["m2_vigente"] for x in linhas if x["condicao"] == "C5")
    reg = sorted(x["m2_registrada"] for x in linhas if x["condicao"] == "C5")
    print(f"\nM2-r em C5 (/33):      min={vig[0]}  mediana={statistics.median(vig)}  max={vig[-1]}")
    print(f"M2 registrada em C5 (/34): min={reg[0]}  mediana={statistics.median(reg)}  max={reg[-1]}")
    for cond in ("C1", "C2", "C3", "C4"):
        for x in linhas:
            if x["condicao"] == cond:
                print(f"  {cond}: M2-r={x['m2_vigente']}/33   (registrada {x['m2_registrada']}/34)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
