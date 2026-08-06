"""Manifesto de congelamento das execuções preservadas.

Gera (ou confere) `runs-manifest.tsv`: caminho relativo, tamanho e SHA-256 de cada
arquivo sob a raiz das execuções. O manifesto é **versionado no repositório**, de
modo que "nada em `runs/` foi modificado" deixe de ser declaração e passe a ser
conferível por terceiros.

É o primeiro e o último passo da apuração: conferir antes de avaliar e depois de
avaliar é o que estabelece que a avaliação não tocou a evidência.

Uso:
    python freeze_manifest.py gerar   [--runs <raiz>] [--out <arquivo>]
    python freeze_manifest.py conferir [--runs <raiz>] [--out <arquivo>]

`conferir` devolve código de saída != 0 se qualquer arquivo mudou, sumiu ou surgiu.
"""
from __future__ import annotations

import hashlib
import os
import sys

AQUI = os.path.dirname(os.path.abspath(__file__))
RAIZ = os.path.dirname(os.path.dirname(AQUI))

RUNS_ROOT_PADRAO = os.environ.get("RUNS_ROOT", os.path.join(RAIZ, "runs"))
OUT_PADRAO = os.path.join(AQUI, "runs-manifest.tsv")


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


def sha256(caminho: str) -> str:
    h = hashlib.sha256()
    with open(caminho, "rb") as fh:
        for bloco in iter(lambda: fh.read(1 << 20), b""):
            h.update(bloco)
    return h.hexdigest()


def varrer(raiz: str):
    """Devolve [(caminho_relativo, tamanho, hash)] ordenado, com barras normalizadas."""
    itens = []
    for base, dirs, arquivos in os.walk(raiz):
        dirs.sort()
        for nome in sorted(arquivos):
            absoluto = os.path.join(base, nome)
            relativo = os.path.relpath(absoluto, raiz).replace(os.sep, "/")
            try:
                itens.append((relativo, os.path.getsize(absoluto), sha256(absoluto)))
            except OSError as e:
                print(f"!! ilegível: {relativo} ({e})", file=sys.stderr)
    return sorted(itens)


def ler_manifesto(caminho: str):
    linhas = {}
    with open(caminho, encoding="utf-8") as fh:
        cabecalho = fh.readline()
        if not cabecalho.startswith("caminho\t"):
            raise ValueError("manifesto sem cabeçalho esperado")
        for ln in fh:
            partes = ln.rstrip("\n").split("\t")
            if len(partes) == 3:
                linhas[partes[0]] = (int(partes[1]), partes[2])
    return linhas


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] not in ("gerar", "conferir"):
        print(__doc__)
        return 2
    modo = sys.argv[1]
    raiz = arg("--runs", RUNS_ROOT_PADRAO)
    out = arg("--out", OUT_PADRAO)

    if not os.path.isdir(raiz):
        print(f"!! raiz de execuções inexistente: {raiz}")
        return 2

    atual = varrer(raiz)
    print(f"varridos {len(atual)} arquivos em {raiz}")

    if modo == "gerar":
        with open(out, "w", encoding="utf-8", newline="\n") as fh:
            fh.write("caminho\ttamanho\tsha256\n")
            for rel, tam, h in atual:
                fh.write(f"{rel}\t{tam}\t{h}\n")
        print(f"manifesto escrito: {out}")
        return 0

    if not os.path.isfile(out):
        print(f"!! manifesto inexistente: {out}")
        return 2
    esperado = ler_manifesto(out)
    agora = {rel: (tam, h) for rel, tam, h in atual}

    sumiram = sorted(set(esperado) - set(agora))
    surgiram = sorted(set(agora) - set(esperado))
    mudaram = sorted(k for k in set(esperado) & set(agora) if esperado[k] != agora[k])

    for k in sumiram:
        print(f"  SUMIU     {k}")
    for k in surgiram:
        print(f"  SURGIU    {k}")
    for k in mudaram:
        print(f"  MUDOU     {k}")

    if sumiram or surgiram or mudaram:
        print(f"!! CONGELAMENTO VIOLADO — {len(sumiram)} sumiram, {len(surgiram)} surgiram, "
              f"{len(mudaram)} mudaram")
        return 1
    print(f"congelamento íntegro: {len(esperado)} arquivos conferidos.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
