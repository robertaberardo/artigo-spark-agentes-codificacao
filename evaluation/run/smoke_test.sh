#!/usr/bin/env bash
# Smoke test do fluxo de TESTES (3 camadas): storage (MinIO + seed) -> repo-baseline
# (build + pipeline ibge: bronze x2 -> silver -> gold). Exercita a mecânica ponta a
# ponta com as fixtures, sem baixar os microdados nem rodar o experimento completo.
#
# O pipeline ibge é o próprio teste do ambiente: o silver cria geometria via
# SedonaContext e escreve/relê geoparquet no S3A, de modo que Spark, Sedona e o
# conector do lake são exercitados por job real, não por asserção sintética.
#
# Sem 'pipefail' de propósito: usamos `... | grep -q`, que fecha o pipe no primeiro
# casamento (SIGPIPE no spark-submit); com pipefail isso viraria falso negativo.
set -u

RAIZ="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${RAIZ}"
falhas=0
checa() { if [ "$1" -eq 0 ]; then echo "   OK: $2"; else echo "   FALHOU: $2"; falhas=$((falhas+1)); fi; }

echo "== 1) rede + storage (MinIO) =="
docker network create external-net >/dev/null 2>&1 || true
( cd storage && docker compose -f docker-compose.minio.yml up -d >/dev/null 2>&1 )
checa $? "MinIO no ar (storage)"

echo "== 2) seed das fixtures do ibge (via mc, sem Spark) =="
bash storage/scripts/seed_ibge_fixture.sh >/dev/null 2>&1
checa $? "fixtures do ibge semeadas"

echo "== 3) build da imagem (a partir do baseline) =="
( cd repo-baseline && docker compose build spark >/dev/null 2>&1 )
checa $? "imagem construída"

echo "== 4) pipeline real ibge (raw->bronze->silver->gold, usa utils, SALVA) =="
( cd repo-baseline \
  && MSYS_NO_PATHCONV=1 docker compose run --rm spark spark-submit apps/ibge/jobs/bronze/ibge_municipios.py >/dev/null 2>&1 \
  && MSYS_NO_PATHCONV=1 docker compose run --rm spark spark-submit apps/ibge/jobs/bronze/ibge_populacao.py >/dev/null 2>&1 \
  && MSYS_NO_PATHCONV=1 docker compose run --rm spark spark-submit apps/ibge/jobs/silver/ibge_relation_municipio_populacao.py >/dev/null 2>&1 \
  && MSYS_NO_PATHCONV=1 docker compose run --rm spark spark-submit apps/ibge/jobs/gold/ibge_relation_municipio_populacao_por_uf.py 2>&1 \
       | grep -q "gold ibge_relation_municipio_populacao_por_uf" )
checa $? "pipeline ibge (bronze x2 -> silver -> gold) salvou o output"

echo ""
if [ "${falhas}" -eq 0 ]; then
  echo "SMOKE TEST: OK (storage + baseline ponta a ponta)."
else
  echo "SMOKE TEST: ${falhas} falha(s) — verifique acima."
fi
exit "${falhas}"
