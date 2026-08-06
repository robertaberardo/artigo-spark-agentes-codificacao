#!/usr/bin/env bash
# Baixa as bases do Inep usadas no experimento, registra checksum (congela o dado)
# e extrai para uma área de staging local. NÃO faz upload — isso é o load_to_minio.sh.
#
# Uso:   bash storage/scripts/download_inep.sh [ANO]
# Padrão: ANO=2025
#
# Observação sobre o Censo 2025: o Inep publicou o arquivo num caminho com um
# underscore extra no fim (microdados_censo_escolar_2025_.zip). O script lida com
# isso testando a variante correta por ano.
set -euo pipefail

ANO="${1:-2025}"
DIR_STAGING="$(cd "$(dirname "$0")/.." && pwd)/_staging"
MANIFESTO="${DIR_STAGING}/manifest.sha256"
mkdir -p "${DIR_STAGING}"

baixa_e_congela() {
  # $1 = url, $2 = nome do arquivo de destino
  local url="$1" destino="${DIR_STAGING}/$2"
  echo ">> baixando: ${url}"
  if ! curl -fSL --retry 3 -o "${destino}" "${url}"; then
    echo "!! FALHA no download de ${url}" >&2
    return 1
  fi
  # congela o dado: registra o sha256 para reprodutibilidade
  ( cd "${DIR_STAGING}" && sha256sum "$2" | tee -a "${MANIFESTO}" )
}

# --- Censo Escolar da Educação Básica -------------------------------------
# O nome do arquivo varia; 2025 tem o underscore extra. Tenta as variantes.
censo_ok=0
for nome in "microdados_censo_escolar_${ANO}_.zip" "microdados_censo_escolar_${ANO}.zip"; do
  url="https://download.inep.gov.br/dados_abertos/${nome}"
  if curl -fsIL "${url}" >/dev/null 2>&1; then
    baixa_e_congela "${url}" "censo_escolar_${ANO}.zip" && censo_ok=1 && break
  fi
done
if [ "${censo_ok}" -eq 1 ]; then
  echo ">> extraindo Censo ${ANO}"
  rm -rf "${DIR_STAGING}/censo_escolar_${ANO}"
  unzip -q -o "${DIR_STAGING}/censo_escolar_${ANO}.zip" -d "${DIR_STAGING}/censo_escolar_${ANO}"
else
  echo "!! Censo ${ANO} indisponível no Inep — pulando (verifique o ano)." >&2
fi

# --- Catálogo de Escolas (coordenadas p/ geoespacial) ---------------------
# RESOLVIDO: a Tabela_Escola do Censo já traz LATITUDE e LONGITUDE (cols 34/35),
# além de toda a hierarquia geográfica (região, UF, município, meso/microrregião,
# CEP, endereço). Logo, o Catálogo de Escolas (painel BI, sem .zip direto) NÃO é
# necessário: a parte geoespacial usa lat/long da Tabela_Escola, e os joins
# multi-fonte saem das próprias tabelas do Censo (Escola × Matrícula × Turma ×
# Docente via CO_ENTIDADE). Mantido aqui apenas como registro da decisão.
echo ">> Catálogo de Escolas: dispensado (lat/long já vêm da Tabela_Escola do Censo)."

echo ">> pronto. Staging em: ${DIR_STAGING}"
