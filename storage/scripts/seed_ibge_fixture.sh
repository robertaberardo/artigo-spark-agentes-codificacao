#!/usr/bin/env bash
# Sobe as fixtures ESTÁTICAS do app `ibge` (dados sujos propositais) no MinIO, nos
# prefixos raw que os jobs bronze leem. Duas fontes que o silver junta:
#   ibge_municipios.csv -> s3://<bucket>/ibge/ibge_municipios/raw/
#   ibge_populacao.csv  -> s3://<bucket>/ibge/ibge_populacao/raw/
# Convenção de path: s3://<bucket>/<app>/<tabela>/<nível>/ (tabela sempre com o
# prefixo do app). Usa o cliente `mc` num container efêmero na rede externa
# `external-net` — NÃO depende da imagem do Spark, então o storage fica
# independente do baseline Spark.
#
# Pré-requisitos: docker network create external-net  +  MinIO no ar
#                 (docker compose -f storage/docker-compose.minio.yml up -d)
# Uso:            bash storage/scripts/seed_ibge_fixture.sh
set -euo pipefail

RAIZ="$(cd "$(dirname "$0")/.." && pwd)"           # storage/
FIXTURES="${RAIZ}/fixtures"
BUCKET="${S3_BUCKET:-datalake}"
USER_MINIO="${MINIO_ROOT_USER:-admin}"
PASS_MINIO="${MINIO_ROOT_PASSWORD:-admin1234}"

# tabela -> arquivo de fixture. O prefixo raw é ibge/<tabela>/raw/.
TABELAS="ibge_municipios ibge_populacao"

for tabela in ${TABELAS}; do
  [ -f "${FIXTURES}/${tabela}.csv" ] || { echo "!! fixture não encontrada: ${FIXTURES}/${tabela}.csv" >&2; exit 1; }
done

echo ">> semeando fixtures do ibge -> s3://${BUCKET}/ibge/<tabela>/raw/"
MSYS_NO_PATHCONV=1 docker run --rm --network external-net \
  -v "${FIXTURES}:/fixtures:ro" \
  --entrypoint sh minio/mc:RELEASE.2024-09-16T17-43-14Z -c "
    until mc alias set local http://s3:9000 ${USER_MINIO} ${PASS_MINIO} >/dev/null 2>&1; do echo 'aguardando s3...'; sleep 2; done
    mc mb --ignore-existing local/${BUCKET}
    for tabela in ${TABELAS}; do
      prefixo=\"ibge/\${tabela}/raw\"
      mc rm --recursive --force \"local/${BUCKET}/\${prefixo}/\" >/dev/null 2>&1 || true
      mc cp \"/fixtures/\${tabela}.csv\" \"local/${BUCKET}/\${prefixo}/\${tabela}.csv\"
      echo \"--- \${tabela} carregada ---\"
      mc ls \"local/${BUCKET}/\${prefixo}/\"
    done
  "
echo ">> fixtures do ibge semeadas."
