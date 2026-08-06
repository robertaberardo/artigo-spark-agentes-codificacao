#!/usr/bin/env bash
# Prepara uma execução isolada: copia o repo-baseline para runs/<run_id>/workdir,
# aplica o overlay do Ti (se houver) e cria um CLAUDE_CONFIG_DIR efêmero (memória
# zerada) e uma pasta de auditoria FORA do workdir. Ao final, imprime o comando
# para iniciar a sessão do Claude Code já isolada.
#
# Uso: bash evaluation/run/new_run.sh <run_id> [overlay]
set -euo pipefail

RAIZ="$(cd "$(dirname "$0")/../.." && pwd)"
RUN_ID="${1:?uso: new_run.sh <run_id> [overlay]}"
OVERLAY="${2:-}"
BASE="${RAIZ}/repo-baseline"

[ -d "${BASE}" ] || { echo "!! baseline ausente: repo-baseline/ e material versionado do experimento." >&2; exit 1; }

# Execução NOVA escreve FORA do repo (default D:/runs) para que o workdir do agente
# não tenha o repo do experimento como ancestral — é o requisito de isolamento.
# Não confundir com `runs/` no repo, que é a evidência preservada. Override via RUNS_ROOT.
if [ -z "${RUNS_ROOT:-}" ]; then
  grep -qi microsoft /proc/version 2>/dev/null && RUNS_ROOT=/mnt/d/runs || RUNS_ROOT=/d/runs
fi
RUN_DIR="${RUNS_ROOT}/${RUN_ID}"
rm -rf "${RUN_DIR}"
mkdir -p "${RUN_DIR}/workdir" "${RUN_DIR}/claude-home" "${RUN_DIR}/audit"

# copia o baseline (byte-idêntico) para o workdir da run
tar -cf - -C "${BASE}" . | tar -xf - -C "${RUN_DIR}/workdir"

# aplica o overlay do Ti por cima, se informado
if [ -n "${OVERLAY}" ]; then
  if [ -d "${RAIZ}/overlays/${OVERLAY}" ]; then
    tar -cf - -C "${RAIZ}/overlays/${OVERLAY}" . | tar -xf - -C "${RUN_DIR}/workdir"
    echo ">> overlay aplicado: ${OVERLAY}"
  else
    echo "!! overlay '${OVERLAY}' não encontrado em overlays/ — seguindo só com o baseline." >&2
  fi
fi

echo ">> run preparada: ${RUN_DIR}"
echo ">> inicie a sessão isolada assim (rodando de dentro do workdir):"
echo ""
echo "   cd '${RUN_DIR}/workdir'"
echo "   CLAUDE_CONFIG_DIR='${RUN_DIR}/claude-home' \\"
echo "   HOOK_LOG_DIR='${RUN_DIR}/audit' \\"
echo "   claude"
echo ""
echo ">> ao terminar a sessão, rode: bash evaluation/run/reset.sh ${RUN_ID}"
