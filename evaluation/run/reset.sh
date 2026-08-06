#!/usr/bin/env bash
# Fecha uma execução: coleta os transcripts do config efêmero (ANTES de qualquer
# teardown) para a pasta de auditoria da run e limpa o prefixo de output desta
# run no MinIO, preservando os dados-fonte (raw/). Não altera arquivos originais.
#
# Uso: bash evaluation/run/reset.sh <run_id>
set -euo pipefail

RAIZ="$(cd "$(dirname "$0")/../.." && pwd)"
RUN_ID="${1:?uso: reset.sh <run_id>}"
# Execução NOVA vive FORA do repo (default D:/runs); `runs/` no repo é a evidência
# preservada e não se mexe. Override via RUNS_ROOT (precisa bater com o usado no
# new_run.sh/run_experiment.sh desta run).
if [ -z "${RUNS_ROOT:-}" ]; then
  grep -qi microsoft /proc/version 2>/dev/null && RUNS_ROOT=/mnt/d/runs || RUNS_ROOT=/d/runs
fi
RUN_DIR="${RUNS_ROOT}/${RUN_ID}"
BUCKET="${S3_BUCKET:-datalake}"

# 1) coleta os transcripts da sessão (config efêmero) para a auditoria da run
if [ -d "${RUN_DIR}/claude-home/projects" ]; then
  rm -rf "${RUN_DIR}/audit/transcripts"
  cp -r "${RUN_DIR}/claude-home/projects" "${RUN_DIR}/audit/transcripts"
  echo ">> transcripts coletados em ${RUN_DIR}/audit/transcripts"
else
  echo "!! sem transcripts em ${RUN_DIR}/claude-home/projects (sessão rodou?)." >&2
fi

# 2) limpa o output desta run no MinIO (mantém raw/)
docker run --rm --network external-net --entrypoint sh \
  minio/mc:RELEASE.2024-09-16T17-43-14Z -c "
    mc alias set local http://s3:9000 ${MINIO_ROOT_USER:-admin} ${MINIO_ROOT_PASSWORD:-admin1234} >/dev/null 2>&1
    mc rm --recursive --force local/${BUCKET}/runs/${RUN_ID}/ 2>/dev/null || true
    echo '>> output da run limpo no MinIO (raw/ preservado).'
  "
