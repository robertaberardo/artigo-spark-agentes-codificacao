"""Monta o `checklist.tsv` da execução a partir das saídas dos instrumentos.

Todos os instrumentos emitem linhas no padrão `X.Y[.Z] <rótulo?> PASS|FAIL <evidência>`:

  * oráculo (`full_compare.py` / `score_compare.py`) -> 1.1 .. 1.8   (correção)
  * `schema_fields.py`                                 -> 2.1.1 .. 2.1.2
  * `reuse_check.py`                                   -> 2.2.1 .. 2.2.10
  * `style_check.py`                                   -> 2.3.1 .. 2.3.16
  * `convention.py`                                    -> 2.4.1 .. 2.4.5

São **41 verificações pontuadas** (8 + 33). Além delas, os instrumentos emitem as
**variáveis registradas** no padrão `VAR <nome> <valor>  <evidência>  [registrado <id>]`:
conformidade de CRS, casas decimais por coluna e uso de `deduplicate_by_key`. Elas
**não pontuam**, mas o **gate de completude cobre pontuados e variáveis**: variável
ausente invalida a execução tanto quanto item não avaliado. É a materialização, no
artefato, da promessa de que nada é descartado em silêncio.

Podem aparecer ainda `GATE <nome> PASS|FAIL` e `AVISO <nome>: <texto>`. Gates não
entram na contagem; hoje o `GATE schema_contrato` exige as 8 colunas contratadas na
saída e integra a correção binária. Avisos são só reportados.

O artefato tem a coluna `registrado`, com o identificador correspondente na rubrica
registrada, para o crosswalk ser gerado por código: os identificadores de reúso **não
são estáveis** entre as duas composições (a partir de `2.2.6` um mesmo número designa
itens diferentes).

Uso:
    python build_checklist.py --audit-dir <dir> --out <checklist.tsv>
"""
from __future__ import annotations

import os
import re
import sys

LINE = re.compile(r"^(?P<id>\d+(?:\.\d+){1,2})\b(?P<meio>.*?)\b(?P<verdict>PASS|FAIL)\b(?P<ev>.*)$")
GATE = re.compile(r"^GATE\s+(?P<name>\S+)\s+(?P<verdict>PASS|FAIL)\b(?P<ev>.*)$")
WARN = re.compile(r"^AVISO\s+(?P<name>\S+?):?\s+(?P<text>.*)$")
VAR = re.compile(r"^VAR\s+(?P<name>\S+)\s+(?P<value>\S+)(?P<ev>.*)$")
REG = re.compile(r"\[registrado\s+(?P<id>[^\]]+)\]")

REQUIRED_GATES = ["schema_contrato"]
VARIAVEIS_ESPERADAS = ["crs", "casas", "dedupe"]

# Correspondência com a rubrica registrada, para as verificações cujo identificador
# mudou ou que não têm contraparte. As não listadas mantêm o próprio identificador.
REGISTRADO_PADRAO = {
    "2.2.6": "2.2.7", "2.2.7": "2.2.8", "2.2.8": "2.2.9",
    "2.2.9": "2.2.10+2.2.11", "2.2.10": "2.2.12",
    "2.4.5": "—",
}


def expected_items() -> list[str]:
    items = [f"1.{i}" for i in range(1, 9)]           # 8 correção
    items += ["2.1.1", "2.1.2"]                        # 2 esquema
    items += [f"2.2.{i}" for i in range(1, 11)]        # 10 reúso (classes)
    items += [f"2.3.{i}" for i in range(1, 17)]        # 16 estilo
    items += [f"2.4.{i}" for i in range(1, 6)]         # 5 convenção
    return items


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


INSTRUMENT_FILES = {
    "schema_fields.txt", "reuse_check.txt", "style_check.txt", "convention.txt",
    "metrics_absent.txt",
}


def is_instrument(name: str) -> bool:
    return name in INSTRUMENT_FILES or name.startswith("oracle_")


def scan(audit_dir: str):
    """(itens, variáveis, gates, avisos) lidos dos arquivos de instrumento.

    Só varre arquivos de INSTRUMENTO, nunca a saída do agente, para não capturar um
    'PASS' incidental. A primeira ocorrência de cada identificador vence.
    """
    found: dict[str, tuple[str, str, str, str]] = {}
    variaveis: dict[str, tuple[str, str, str, str]] = {}
    gates: dict[str, tuple[str, str, str]] = {}
    warns: list[tuple[str, str, str]] = []
    if not os.path.isdir(audit_dir):
        return found, variaveis, gates, warns
    for name in sorted(os.listdir(audit_dir)):
        if not name.endswith(".txt") or not is_instrument(name):
            continue
        try:
            lines = open(os.path.join(audit_dir, name), encoding="utf-8",
                         errors="replace").read().splitlines()
        except OSError:
            continue
        for ln in lines:
            ln = ln.strip()
            g = GATE.match(ln)
            if g:
                gates.setdefault(g.group("name"),
                                 (g.group("verdict"), g.group("ev").strip(), name))
                continue
            w = WARN.match(ln)
            if w:
                warns.append((w.group("name"), w.group("text").strip(), name))
                continue
            v = VAR.match(ln)
            if v:
                ev = v.group("ev").strip()
                reg = REG.search(ev)
                variaveis.setdefault(v.group("name"),
                                     (v.group("value"), REG.sub("", ev).strip(),
                                      name, reg.group("id") if reg else "—"))
                continue
            m = LINE.match(ln)
            if not m:
                continue
            iid = m.group("id")
            if iid in found:
                continue
            ev = m.group("ev").strip()
            reg = REG.search(m.group("meio") or "") or REG.search(ev)
            found[iid] = (m.group("verdict"), REG.sub("", ev).strip(), name,
                          reg.group("id") if reg else REGISTRADO_PADRAO.get(iid, iid))
    return found, variaveis, gates, warns


def _sob_raiz_de_execucoes(caminho: str) -> bool:
    """Guarda do princípio de somente-leitura: recusa escrever dentro de `runs/`.

    Boa intenção não protege dado: `--out` aceitaria qualquer caminho, e um engano de
    linha de comando sobrescreveria a evidência congelada. A raiz é configurável por
    `RUNS_ROOT` para não prender o teste ao caminho da máquina.
    """
    _raiz_repo = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    raiz = os.environ.get("RUNS_ROOT", os.path.join(_raiz_repo, "runs"))
    try:
        return os.path.commonpath([os.path.abspath(caminho), os.path.abspath(raiz)]) \
            == os.path.abspath(raiz)
    except ValueError:
        return False


def main() -> int:
    audit_dir = arg("--audit-dir")
    out = arg("--out")
    if not audit_dir or not out:
        print(__doc__)
        return 2
    if not os.path.isdir(audit_dir):
        print(f"!! audit-dir inexistente: {audit_dir}")
        return 2
    if _sob_raiz_de_execucoes(out):
        print(f"!! RECUSADO: {out} está sob a raiz das execuções, que é somente leitura.")
        return 2

    found, variaveis, gates, warns = scan(audit_dir)
    expected = expected_items()
    missing = [i for i in expected if i not in found]
    var_missing = [v for v in VARIAVEIS_ESPERADAS if v not in variaveis]

    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    with open(out, "w", encoding="utf-8", newline="\n") as fh:
        fh.write("item\ttipo\tresultado\tregistrado\tfonte\tevidencia\n")
        for iid in expected:
            if iid in found:
                v, ev, src, reg = found[iid]
                fh.write(f"{iid}\tpontuado\t{v}\t{reg}\t{src}\t{ev}\n")
            else:
                fh.write(f"{iid}\tpontuado\tNAO_AVALIADO\t"
                         f"{REGISTRADO_PADRAO.get(iid, iid)}\t-\t-\n")
        for nome in VARIAVEIS_ESPERADAS:
            if nome in variaveis:
                val, ev, src, reg = variaveis[nome]
                fh.write(f"{nome}\tregistrada\t{val}\t{reg}\t{src}\t{ev}\n")
            else:
                fh.write(f"{nome}\tregistrada\tNAO_AVALIADA\t—\t-\t-\n")

    def rate(prefix, total):
        ids = [i for i in expected if i.startswith(prefix)]
        return sum(1 for i in ids if i in found and found[i][0] == "PASS"), total

    m1_ids = [f"1.{i}" for i in range(1, 9)]
    m1_pass = sum(1 for i in m1_ids if found.get(i, ("",))[0] == "PASS")
    gates_ok = all(gates.get(g, ("FAIL",))[0] == "PASS" for g in REQUIRED_GATES)
    correcao = (m1_pass == 8) and gates_ok and not missing
    s = rate("2.1", 2); r = rate("2.2", 10); e = rate("2.3", 16); c = rate("2.4", 5)
    ctx_pass = s[0] + r[0] + e[0] + c[0]

    print(f"checklist.tsv escrito: {out}")
    print(f"M1 (correção binária): {'CORRETA' if correcao else 'INCORRETA'} ({m1_pass}/8)")
    for g in REQUIRED_GATES:
        v, ev, src = gates.get(g, ("AUSENTE", "instrumento não emitiu o gate", "-"))
        print(f"  gate {g}: {v}  [{src}] {ev}")
    for nome in VARIAVEIS_ESPERADAS:
        val = variaveis.get(nome, ("AUSENTE",))[0]
        print(f"  var {nome}: {val}")
    for name, text, src in warns:
        print(f"  AVISO {name}: {text}  [{src}]")
    print(f"M2-r (aderência): {ctx_pass}/33  "
          f"[esquema {s[0]}/2 · reúso {r[0]}/10 · estilo {e[0]}/16 · convenção {c[0]}/5]")

    if missing or var_missing:
        print(f"!! GATE DE COMPLETUDE FALHOU — itens não avaliados: {missing or '—'}; "
              f"variáveis ausentes: {var_missing or '—'}")
        return 1
    print("gate de completude: 41/41 pontuados + 3/3 variáveis.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
