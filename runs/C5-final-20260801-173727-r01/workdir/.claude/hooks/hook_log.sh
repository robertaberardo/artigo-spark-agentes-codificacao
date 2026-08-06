#!/usr/bin/env bash
# Logger de hook do Claude Code. Anexa o JSON recebido no stdin a um arquivo de
# log e NÃO imprime nada no stdout — de propósito: a saída de um hook
# UserPromptSubmit iria para o contexto do modelo. Aqui só gravamos.
#
# Cobre:
#   - PostToolUse      -> toda chamada de ferramenta (entrada + saída completas)
#   - UserPromptSubmit -> o prompt completo, já com colagens expandidas
#
# Destino: HOOK_LOG_DIR quando definido (fora do workdir); senão, diretório local.
set -euo pipefail
DIR="${HOOK_LOG_DIR:-${CLAUDE_PROJECT_DIR:-.}/.audit_logs}"
mkdir -p "$DIR"
# Anexa o payload do stdin + uma quebra de linha (fluxo JSON concatenado).
{ cat; printf '\n'; } >> "$DIR/hooks.jsonl"
exit 0
