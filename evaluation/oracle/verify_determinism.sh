#!/usr/bin/env bash
# Determinismo do gabarito (§5): regenera 2x e exige MATCH (a 6 casas) — pega
# soma float fora de ordem / não-determinismo antes de congelar o gabarito.
# Uso: bash evaluation/oracle/verify_determinism.sh
set -uo pipefail

RAIZ="$(cd "$(dirname "$0")/../.." && pwd)"
GAB="${RAIZ}/evaluation/gabaritos"
A="${GAB}/_det_A"
B="${GAB}/_det_B"

submit() {  # spark-submit no container: evaluation/ (ro, p/ schema_source) + saída rw
  (cd "${RAIZ}/repo-baseline" && MSYS_NO_PATHCONV=1 docker compose run --rm \
     -v "${RAIZ}/evaluation:/data/evaluation:ro" \
     -v "${GAB}:/data/gabaritos" \
     spark spark-submit "$@")
}

echo ">> réplica A..."
submit /data/evaluation/gabaritos/build_gabarito.py --out /data/gabaritos/_det_A >/dev/null 2>&1
echo ">> réplica B..."
submit /data/evaluation/gabaritos/build_gabarito.py --out /data/gabaritos/_det_B >/dev/null 2>&1

echo ">> comparando A x B (full_compare, round=6)..."
submit /data/evaluation/oracle/full_compare.py /data/gabaritos/_det_A /data/gabaritos/_det_B \
  --key h3_cell --geom geometry --round 6
EC=$?

rm -rf "${A}" "${B}" 2>/dev/null || \
  submit /bin/sh -c 'rm -rf /data/gabaritos/_det_A /data/gabaritos/_det_B' >/dev/null 2>&1 || true

if [ ${EC} -eq 0 ]; then
  echo "DETERMINISMO: OK (gabarito idêntico em 2 gerações, a 6 casas)."
else
  echo "DETERMINISMO: FALHOU — o gabarito não é reprodutível (ver saída acima)."
fi
exit ${EC}
