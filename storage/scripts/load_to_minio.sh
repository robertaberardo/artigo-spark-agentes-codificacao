#!/usr/bin/env bash
# Semeia o raw DE-BRANDADO (saída de debrand_censo.py) no MinIO externo, por tabela:
#     s3://<bucket>/educacao/<tabela>/raw/ano=<ANO>/<tabela>.csv
# Do lake para baixo só existem os nomes de-brandados (SETOR/SITUACAO/MAT_BASICA/…);
# os nomes originais do Censo ficam só no RAW_RENAME de evaluation/schema_source.py.
#
# Pré-requisitos:
#   1. docker network create external-net  +  MinIO no ar
#   2. python storage/scripts/debrand_censo.py  (gera _staging/debranded/)
# Uso: bash storage/scripts/load_to_minio.sh [ANO]
set -euo pipefail

ANO="${1:-2025}"
DIR_DEBRANDED="$(cd "$(dirname "$0")/.." && pwd)/_staging/debranded"
BUCKET="${S3_BUCKET:-datalake}"
USER_MINIO="${MINIO_ROOT_USER:-admin}"
PASS_MINIO="${MINIO_ROOT_PASSWORD:-admin1234}"

[ -d "${DIR_DEBRANDED}" ] || {
  echo "!! _staging/debranded ausente — rode 'python storage/scripts/debrand_censo.py' antes." >&2
  exit 1
}

echo ">> semeando raw de-brandado (${DIR_DEBRANDED}) -> s3://${BUCKET}/educacao/<tabela>/raw/ano=${ANO}/"
MSYS_NO_PATHCONV=1 docker run --rm --network external-net \
  -e ANO="${ANO}" -e BUCKET="${BUCKET}" \
  -e USER_MINIO="${USER_MINIO}" -e PASS_MINIO="${PASS_MINIO}" \
  -v "${DIR_DEBRANDED}:/debranded:ro" \
  --entrypoint sh minio/mc:RELEASE.2024-09-16T17-43-14Z -c '
    until mc alias set local http://s3:9000 "$USER_MINIO" "$PASS_MINIO" >/dev/null 2>&1; do echo "aguardando s3..."; sleep 2; done
    mc mb --ignore-existing "local/$BUCKET"
    # de-brand total: zera o prefixo educacao/ (remove o censo-headed antigo E as
    # tabelas-ruído docente/gestor/curso_tecnico, que ainda teriam header do Censo).
    mc rm --recursive --force "local/$BUCKET/educacao/" >/dev/null 2>&1 || true
    for f in /debranded/*.csv; do
      nome=$(basename "$f"); tabela=${nome%.csv}
      prefixo="educacao/$tabela/raw/ano=$ANO"
      mc cp "$f" "local/$BUCKET/$prefixo/$nome"
      echo "  $nome -> $prefixo/"
    done
    echo "--- conteudo carregado ---"
    mc ls --recursive "local/$BUCKET/educacao/"
  '
echo ">> carga concluída."
