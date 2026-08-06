#!/usr/bin/env bash
# §5b — VALIDAÇÃO DOS INSTRUMENTOS antes de qualquer run real: prova que os
# testes pegam o que devem, alimentando-os com saídas propositalmente erradas.
#   (1) métricas estáticas (schema/reúso/estilo/convenção) + gate de completude
#       -> pytest (fixtures boas/ruins).
#   (2) oráculo -> variantes com UM erro plantado; cada uma reprova no item certo.
# Uso: bash evaluation/run/validate_instruments.sh
set -uo pipefail

RAIZ="$(cd "$(dirname "$0")/../.." && pwd)"
GAB="${RAIZ}/evaluation/gabaritos"
TARGET="${GAB}/educacao_escola_matricula_h3_grid"
PY="$(command -v python || command -v python3 || echo python)"

echo "==================== §5b (1) MÉTRICAS + gate (pytest) ===================="
"${PY}" -m pytest "${RAIZ}/evaluation/metrics/tests" -q || { echo "!! PYTEST FALHOU"; exit 1; }

echo ""
echo "==================== §5b (2) ORÁCULO (erros plantados) ==================="
[ -d "${TARGET}" ] || { echo "!! gabarito ausente: ${TARGET}"; exit 1; }

submit() {
  (cd "${RAIZ}/repo-baseline" && MSYS_NO_PATHCONV=1 docker compose run --rm \
     -v "${RAIZ}/evaluation:/data/evaluation:ro" -v "${GAB}:/data/gabaritos" \
     spark spark-submit "$@")
}

echo ">> gerando variantes com erro plantado..."
submit /data/evaluation/oracle/_make_wrong.py \
  --gab /data/gabaritos/educacao_escola_matricula_h3_grid \
  --out /data/gabaritos/_wrong >/dev/null 2>&1

# caso -> item que DEVE reprovar
CASOS="zero_for_null:1.6 wrong_total:1.3 isolated_nonzero:1.5"
FAIL=0
for pair in ${CASOS}; do
  caso="${pair%%:*}"; exp="${pair##*:}"
  log="${GAB}/_wrong/${caso}.oracle.txt"
  submit /data/evaluation/oracle/full_compare.py \
    "/data/gabaritos/_wrong/${caso}" \
    /data/gabaritos/educacao_escola_matricula_h3_grid \
    --key h3_cell --geom geometry --round 2 > "${log}" 2>&1
  got="$(grep -E "^${exp} " "${log}" | grep -oE 'PASS|FAIL' | head -1)"
  # a run inteira também deve ser MISMATCH (correção binária = incorreta)
  ratio="$(grep -E '^RATIO 1' "${log}" | tail -1)"
  if [ "${got}" = "FAIL" ]; then
    echo "  OK  ${caso}: item ${exp} = FAIL (pego)   [${ratio}]"
  else
    echo "  !!  ${caso}: item ${exp} = ${got:-nao-encontrado} (NAO pego)   [${ratio}]"
    FAIL=$((FAIL+1))
  fi
done

rm -rf "${GAB}/_wrong" 2>/dev/null || \
  submit /bin/sh -c 'rm -rf /data/gabaritos/_wrong' >/dev/null 2>&1 || true

echo ""
if [ ${FAIL} -eq 0 ]; then
  echo ">> §5b OK — os instrumentos pegam cada erro plantado no ponto certo."
else
  echo ">> §5b FALHOU — ${FAIL} caso(s) não pego(s)."
fi
exit ${FAIL}
