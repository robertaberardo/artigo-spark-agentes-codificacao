"""Ofuscação do raw (D-3): projeta + renomeia os headers das tabelas-fonte.

Lê o raw COMPLETO (302/237 colunas, `;`, ISO-8859-1) do staging, PROJETA para as
colunas contratadas e RENOMEIA os headers pelo mapa `RAW_RENAME` da fonte única
(`evaluation/schema_source.py`). Preserva os VALORES brutos célula-a-célula
(encoding, separador, brancos, duplicatas, códigos 1/2/3/4) — nada de limpeza.

Do lake para baixo só existem os nomes de-brandados; os nomes originais (a
impressão-digital que o agente reconstruía do prior) somem do dado semeado.

É a "prova" executável do de-brand: imprime cada `ORIGINAL -> DE-BRANDADO`, o nº de
colunas projetadas e de linhas preservadas, e confere que nenhum header original
sobrou.

Uso:
    python storage/scripts/debrand_censo.py [--ano 2025] [--in <dados/>] [--out <dir>]
Saída: `storage/_staging/debranded/educacao_{escola,matricula,turma}.csv`.
"""
from __future__ import annotations

import glob
import os
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "evaluation"))
import schema_source as S  # noqa: E402

# tabela -> padrão do arquivo no staging (mesmo roteamento do load_to_minio.sh)
FILE_GLOB = {
    "escola": "Tabela_Escola_*.csv",
    "matricula": "Tabela_Matricula_*.csv",
    "turma": "Tabela_Turma_*.csv",
}
RAW_OPTS = dict(sep=";", encoding="latin-1", dtype=str, keep_default_na=False, na_filter=False)


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


def find_dados(ano: str) -> str:
    override = arg("--in")
    if override:
        return override
    staging = os.path.join(REPO, "storage", "_staging", f"censo_escolar_{ano}")
    for root, dirs, _files in os.walk(staging):
        if os.path.basename(root) == "dados":
            return root
    raise SystemExit(f"!! pasta 'dados' não encontrada em {staging} (rode download_inep.sh)")


def debrand_table(table: str, dados_dir: str, out_dir: str) -> None:
    matches = glob.glob(os.path.join(dados_dir, FILE_GLOB[table]))
    if not matches:
        print(f"[{table}] AVISO: nenhum CSV casando {FILE_GLOB[table]} — pulado")
        return
    src = matches[0]
    rename = S.RAW_RENAME[table]

    # header real (1ª linha) — sem carregar o arquivo todo
    header = pd.read_csv(src, **{**RAW_OPTS, "nrows": 0}).columns.tolist()
    present = [c for c in rename if c in header]      # projeta na ORDEM do mapa
    missing = [c for c in rename if c not in header]
    faltam_criticas = [c for c in S.CRITICAL_RAW[table] if c not in header]
    if faltam_criticas:
        raise SystemExit(f"!! [{table}] colunas CRÍTICAS ausentes no raw: {faltam_criticas}")
    if missing:
        print(f"[{table}] AVISO: ruído ausente no raw (ignorado): {missing}")

    df = pd.read_csv(src, usecols=present, **RAW_OPTS)[present]  # reordena p/ ordem do mapa
    df = df.rename(columns=rename)

    os.makedirs(out_dir, exist_ok=True)
    dest = os.path.join(out_dir, f"{S.APP}_{table}.csv")
    df.to_csv(dest, sep=";", encoding="latin-1", index=False, lineterminator="\n")

    # prova do ofuscamento
    print(f"\n[{table}] {os.path.basename(src)}  ->  {os.path.basename(dest)}")
    print(f"  colunas projetadas: {len(present)} (de {len(header)})   linhas: {len(df)}")
    for old, new in rename.items():
        if old in present:
            print(f"    {old:<28} -> {new}")
    changed = {old for old, new in rename.items() if old != new}  # LAT/LON mapeiam p/ si
    remanescente = [c for c in df.columns if c in changed]
    assert not remanescente, f"header original vazou: {remanescente}"
    print(f"  0 headers originais no de-brandado: OK")


def main() -> int:
    ano = arg("--ano", "2025")
    dados_dir = find_dados(ano)
    out_dir = arg("--out", os.path.join(REPO, "storage", "_staging", "debranded"))
    print("=" * 70)
    print(f"OFUSCAÇÃO DO RAW (D-3)  ano={ano}")
    print(f"  entrada: {dados_dir}")
    print(f"  saída  : {out_dir}")
    print("=" * 70)
    for table in RAW_RENAME_ORDER:
        debrand_table(table, dados_dir, out_dir)
    print("\n>> ofuscação concluída.")
    return 0


RAW_RENAME_ORDER = ["escola", "matricula", "turma"]

if __name__ == "__main__":
    sys.exit(main())
