#!/usr/bin/env bash
# Orquestrador ÚNICO do experimento: executa N runs de uma configuração, do
# lake limpo ao relatório, sem intervenção manual.
#
# Fluxo por run (D3/D8/P3):
#   1. wipe do lake (preserva só */raw/) + guarda de lake-limpo
#      + down do container do spark da raiz (agente sobe a própria stack)
#   2. new_run.sh          (baseline congelado + overlay da condição -> workdir isolado)
#   3. claude -p           (headless, bypassPermissions, hooks de captura)
#   4. coleta transcripts  (config efêmero -> audit/)
#   5. export              (mc mirror do bucket -> runs/<id>/outputs/, sem raw;
#                           a árvore exportada preserva app/tabela/camada)
#   6. teardown            (wipe de novo — lake volta a só-raw; down do spark)
#   7. validação           (oráculo sobre os ARQUIVOS EXPORTADOS + métricas +
#                           auditoria; cada etapa é best-effort e registrada)
#
# Uso:
#   bash evaluation/run/run_experiment.sh <overlay> [n_runs] [--dry-run]
#     <overlay>   pasta em overlays/ (ex.: C1-baseline)
#     [n_runs]    default 1 (C5-final usou 30)
#     --dry-run   pula a invocação do agente (testa o encanamento)
set -uo pipefail

RAIZ="$(cd "$(dirname "$0")/../.." && pwd)"
OVERLAY="${1:?uso: run_experiment.sh <overlay> [n_runs] [--dry-run]}"
N_RUNS="${2:-1}"
DRY_RUN=0
[[ "${*}" == *"--dry-run"* ]] && DRY_RUN=1
case "${N_RUNS}" in --dry-run) N_RUNS=1 ;; esac

# Execução NOVA escreve FORA do repo (default D:/runs) para que o workdir do agente
# não tenha o repo do experimento como ancestral — é o requisito de isolamento.
# Não confundir com `runs/` no repo, que é a evidência preservada, só leitura.
# Exportado para new_run.sh herdar.
if [ -z "${RUNS_ROOT:-}" ]; then
  grep -qi microsoft /proc/version 2>/dev/null && RUNS_ROOT=/mnt/d/runs || RUNS_ROOT=/d/runs
fi
export RUNS_ROOT

BUCKET="${S3_BUCKET:-datalake}"
MC_IMG="minio/mc:RELEASE.2024-09-16T17-43-14Z"
MC_USER="${MINIO_ROOT_USER:-admin}"
MC_PASS="${MINIO_ROOT_PASSWORD:-admin1234}"
PROMPT_FILE="${RAIZ}/overlays/prompt-pipeline.txt"

# --- pré-voo (uma vez) -------------------------------------------------------
[ -d "${RAIZ}/repo-baseline" ] || { echo "!! baseline ausente: repo-baseline/ e material versionado do experimento." >&2; exit 1; }
[ -d "${RAIZ}/overlays/${OVERLAY}" ] || { echo "!! overlay '${OVERLAY}' não existe em overlays/." >&2; exit 1; }
[ -f "${PROMPT_FILE}" ] || { echo "!! prompt ausente: ${PROMPT_FILE}" >&2; exit 1; }
PROMPT="$(cat "${PROMPT_FILE}")"

# CLI do agente: bash não-interativo (ex.: chamado do PowerShell) não carrega o
# profile, então ~/.local/bin pode estar fora do PATH — tenta os caminhos usuais.
# Só é obrigatória fora do --dry-run.
CLAUDE_BIN="$(command -v claude || command -v claude.exe || true)"
[ -z "${CLAUDE_BIN}" ] && [ -x "${HOME}/.local/bin/claude" ] && CLAUDE_BIN="${HOME}/.local/bin/claude"
if [ -z "${CLAUDE_BIN}" ] && [ "${DRY_RUN}" -eq 0 ]; then
  echo "!! CLI 'claude' não encontrada (PATH e ~/.local/bin)." >&2; exit 1
fi

# python para as métricas/auditoria (mesmo problema de PATH do claude)
PYTHON_BIN="$(command -v python || command -v python3 || command -v py || true)"

# WSL: se o claude resolvido for o binário WINDOWS (interop), env vars comuns
# NÃO atravessam a fronteira WSL->Windows — só as listadas em WSLENV (com /p
# para traduzir caminhos). Sem isso o config efêmero e o log dos hooks são
# silenciosamente ignorados (aconteceu no piloto).
if grep -qi microsoft /proc/version 2>/dev/null; then
  export WSLENV="${WSLENV:+${WSLENV}:}CLAUDE_CONFIG_DIR/p:HOOK_LOG_DIR/p"
fi

mc_sh() {  # roda um trecho de shell no container do mc, já autenticado
  MSYS_NO_PATHCONV=1 docker run --rm --network external-net \
    ${MC_MOUNT:-} --entrypoint sh "${MC_IMG}" -c "
      mc alias set local http://s3:9000 '${MC_USER}' '${MC_PASS}' >/dev/null 2>&1 || exit 9
      $1"
}

down_spark() {  # derruba QUALQUER container do compose do spark na raiz, senão
                # o agente reusa o container vivo via 'docker exec' em vez de
                # subir a própria stack (--remove-orphans cobre o MinIO antigo).
  (cd "${RAIZ}/repo-baseline" && MSYS_NO_PATHCONV=1 \
     docker compose down --remove-orphans >/dev/null 2>&1) || true
  # Containers PER-RUN que o agente sobe DENTRO do seu workdir: o compose usa o
  # basename do diretório ("workdir") como nome de projeto → containers
  # "workdir-spark-1" / "workdir-spark-run-*". O `compose down` acima (rodado no
  # RAIZ/repo-baseline) NÃO os alcança; sem isto eles vazam entre runs e batem
  # porta (Spark UI 4040) ou reusam estado stale. Derruba qualquer um remanescente.
  local orfaos
  orfaos="$(docker ps -aq --filter 'name=workdir-spark' 2>/dev/null)"
  [ -n "${orfaos}" ] && docker rm -f ${orfaos} >/dev/null 2>&1 || true
}

wipe_lake() {  # remove TUDO que não é */raw/* e valida que só sobrou raw
  # (sem grep/awk: a imagem do mc é mínima — filtragem via 'case' do shell)
  mc_sh "
    mc find 'local/${BUCKET}' --name '*' 2>/dev/null | while read -r obj; do
      case \"\$obj\" in */raw/*) ;; *) mc rm --force \"\$obj\" >/dev/null 2>&1 ;; esac
    done
    sobra=''
    while read -r obj; do
      case \"\$obj\" in */raw/*) ;; *) sobra=\"\$obj\"; break ;; esac
    done <<EOF_LIST
\$(mc find 'local/${BUCKET}' --name '*' 2>/dev/null)
EOF_LIST
    if [ -n \"\$sobra\" ]; then echo \"!! lake sujo após wipe: \$sobra\"; exit 1; fi
    echo '>> lake limpo (só raw/).'"
}

# --- loop de runs ------------------------------------------------------------
STAMP="$(date +%Y%m%d-%H%M%S)"
FALHAS=0

for i in $(seq 1 "${N_RUNS}"); do
  RUN_ID="${OVERLAY}-${STAMP}-r$(printf '%02d' "${i}")"
  RUN_DIR="${RUNS_ROOT}/${RUN_ID}"
  REPORT="${RUN_DIR}/report.txt"
  echo ""
  echo "================ run ${i}/${N_RUNS}: ${RUN_ID} ================"

  # 1) lake limpo
  wipe_lake || { echo "!! abortando run (wipe falhou — exógeno, ver D7)." >&2; FALHAS=$((FALHAS+1)); continue; }

  # 1b) derruba container do spark da raiz (estado limpo p/ o agente subir a stack)
  down_spark

  # 2) workdir isolado (baseline + overlay)
  bash "${RAIZ}/evaluation/run/new_run.sh" "${RUN_ID}" "${OVERLAY}" >/dev/null
  mkdir -p "${RUN_DIR}"

  # O /init (T4/T5) roda DE VERDADE mesmo em --dry-run: é o passo novo/barato a
  # validar, e o dry-run precisa provar que o CLAUDE.md nasce no workdir. Só a
  # sessão da TAREFA é pulada no dry-run.
  RUN_INIT=0
  INIT_OVERLAYS="${INIT_OVERLAYS:-C4-claude-md-init C5-final}"
  case " ${INIT_OVERLAYS} " in *" ${OVERLAY} "*) RUN_INIT=1 ;; esac
  if [ "${RUN_INIT}" -eq 1 ] && [ -z "${CLAUDE_BIN}" ]; then
    echo "!! CLI 'claude' não encontrada — necessária para o /init de ${OVERLAY}." >&2
    exit 1
  fi

  # 2b) credenciais no config efêmero: SÓ o .credentials.json — memória,
  # settings de usuário e histórico ficam de fora (config nasce zerado e
  # logado). Sem isso o agente morre com "Not logged in". Também no --dry-run
  # quando o /init vai rodar (RUN_INIT=1).
  if [ "${DRY_RUN}" -eq 0 ] || [ "${RUN_INIT}" -eq 1 ]; then
    # Ordem: override explícito > config do repo (fica fresco enquanto a sessão
    # de trabalho está ativa; os de ~/.claude expiram parados — OAuth vencido
    # derrubou uma run T2 em 2026-07-20) > homes usuais.
    CRED_SRC=""
    for c in "${CLAUDE_CRED_SOURCE:-}" \
             "${RAIZ}/_init_repo_claude_config/.credentials.json" \
             "${HOME}/.claude/.credentials.json" \
             /mnt/c/Users/*/.claude/.credentials.json \
             /c/Users/*/.claude/.credentials.json; do
      [ -f "${c}" ] && CRED_SRC="${c}" && break
    done
    if [ -n "${CRED_SRC}" ]; then
      cp "${CRED_SRC}" "${RUN_DIR}/claude-home/.credentials.json"
    else
      echo "!! credenciais não encontradas (defina CLAUDE_CRED_SOURCE) — o agente falhará com 'Not logged in'." >&2
    fi
  fi
  {
    echo "run_id:  ${RUN_ID}"
    echo "overlay: ${OVERLAY}"
    echo "inicio:  $(date -Iseconds)"
  } > "${REPORT}"

  # 2c) /init por configuração (T4/T5): gera o CLAUDE.md DENTRO do workdir da
  # run, em sessão headless SEPARADA, depois do overlay e antes da tarefa — o
  # CLAUDE.md reflete exatamente o repo que o agente da tarefa vai ver, e a
  # geração passa a fazer parte do pipeline medido (uma por run, não congelada).
  # Falha do init (sem CLAUDE.md) = falha exógena: invalida a run (D7).
  if [ "${RUN_INIT}" -eq 1 ]; then
    echo ">> /init rodando (headless; executa também em --dry-run)..."
    # MSYS2_ARG_CONV_EXCL: sem isso o Git Bash converte o ARGUMENTO "/init" num
    # caminho Windows (C:/Program Files/Git/init) e o agente recebe lixo.
    # NÃO usar MSYS_NO_PATHCONV=1 aqui: ele desliga também a conversão das env
    # vars (CLAUDE_CONFIG_DIR ficaria POSIX e o claude cai em "Not logged in") —
    # as duas variantes morderam em dry-runs de 2026-07-20.
    (
      cd "${RUN_DIR}/workdir" &&
      MSYS2_ARG_CONV_EXCL="/init" \
      CLAUDE_CONFIG_DIR="${RUN_DIR}/claude-home" \
      HOOK_LOG_DIR="${RUN_DIR}/audit" \
      "${CLAUDE_BIN}" -p "/init" --permission-mode bypassPermissions
    ) > "${RUN_DIR}/audit/init_stdout.txt" 2>&1
    INIT_EXIT=$?
    echo "init:    exit=${INIT_EXIT}" >> "${REPORT}"
    if [ "${INIT_EXIT}" -ne 0 ] || [ ! -f "${RUN_DIR}/workdir/CLAUDE.md" ]; then
      echo "!! /init falhou ou não gerou CLAUDE.md — run exógena, descartada (D7)." >&2
      echo "init:    FALHOU (sem CLAUDE.md) — run invalida (D7)" >> "${REPORT}"
      FALHAS=$((FALHAS+1)); continue
    fi
    echo ">> CLAUDE.md gerado ($(wc -l < "${RUN_DIR}/workdir/CLAUDE.md") linhas)."
  fi

  # 3) agente headless (hooks logam em audit/; config efêmero = memória zerada)
  if [ "${DRY_RUN}" -eq 1 ]; then
    echo ">> [dry-run] pulando a invocação do agente."
    echo "agente:  DRY-RUN" >> "${REPORT}"
  else
    echo ">> agente rodando (headless)..."
    (
      cd "${RUN_DIR}/workdir" &&
      CLAUDE_CONFIG_DIR="${RUN_DIR}/claude-home" \
      HOOK_LOG_DIR="${RUN_DIR}/audit" \
      "${CLAUDE_BIN}" -p "${PROMPT}" --permission-mode bypassPermissions
    ) > "${RUN_DIR}/audit/agent_stdout.txt" 2>&1
    AGENT_EXIT=$?
    echo "agente:  exit=${AGENT_EXIT}" >> "${REPORT}"
    echo ">> agente terminou (exit=${AGENT_EXIT})."
  fi

  # 4) transcripts do config efêmero -> auditoria
  if [ -d "${RUN_DIR}/claude-home/projects" ]; then
    rm -rf "${RUN_DIR}/audit/transcripts"
    cp -r "${RUN_DIR}/claude-home/projects" "${RUN_DIR}/audit/transcripts"
  elif [ "${DRY_RUN}" -eq 0 ]; then
    echo "!! config efêmero NÃO usado (claude-home vazio) — interop WSL->Windows?" >&2
    echo "config-efemero: NAO USADO (checar WSLENV/interop) — run possivelmente exógena" >> "${REPORT}"
  fi

  # 5) export dos outputs (estrutura do bucket preservada; raw excluído)
  MC_MOUNT="-v ${RUN_DIR}/outputs:/export" \
  mc_sh "mc mirror --exclude '*/raw/*' 'local/${BUCKET}' /export >/dev/null 2>&1; \
         echo \">> exportado: \$(mc find /export --name '*' 2>/dev/null | wc -l) objetos\"" \
    && echo "export:  ok" >> "${REPORT}" || echo "export:  FALHOU" >> "${REPORT}"

  # 6) teardown — lake volta a só-raw e container do spark é derrubado
  wipe_lake >/dev/null && echo "teardown: ok" >> "${REPORT}" || echo "teardown: FALHOU" >> "${REPORT}"
  down_spark

  # 7) validação e métricas (best-effort; oráculo roda sobre os EXPORTADOS)
  echo "--- validação ---" >> "${REPORT}"

  GABARITOS="${RAIZ}/evaluation/gabaritos"
  TARGET_TABLE="educacao_escola_matricula_h3_grid"
  REF="${GABARITOS}/${TARGET_TABLE}"
  oralog="${RUN_DIR}/audit/oracle_${TARGET_TABLE}.txt"

  # 7a) ORÁCULO — SÓ a tabela final entra na correção (Medida 1, itens 1.1–1.9).
  if [ -d "${REF}" ]; then
    cand="$(find "${RUN_DIR}/outputs" -type d -name "${TARGET_TABLE}" | head -n1)"
    if [ -z "${cand}" ]; then
      echo "oraculo: AUSENTE (${TARGET_TABLE} não persistida — falha do agente)" >> "${REPORT}"
      # sem tabela: registra 1.1–1.8 como FAIL (o gate de completude precisa dos 8)
      # e reprova o gate de schema (não há saída para conferir contra o contrato).
      # As variáveis registradas também precisam existir, senão o gate reprova por
      # ausência de variável em vez de por falha do agente.
      : > "${oralog}"
      for k in 1 2 3 4 5 6 7 8; do echo "1.${k} ausente FAIL (tabela nao persistida)" >> "${oralog}"; done
      echo "GATE schema_contrato FAIL (tabela nao persistida)" >> "${oralog}"
      echo "VAR crs INDETERMINADO  sem tabela final  [registrado 1.9]" >> "${oralog}"
      echo "VAR casas INDETERMINADO  sem tabela final" >> "${oralog}"
    else
      leaf="$(find "${cand}" -name '_SUCCESS' -exec dirname {} \; | head -n1)"
      [ -z "${leaf}" ] && leaf="${cand}"
      (cd "${RAIZ}/repo-baseline" && MSYS_NO_PATHCONV=1 docker compose run --rm \
        -v "${RUN_DIR}/outputs:/data/outputs:ro" \
        -v "${GABARITOS}:/data/gabaritos:ro" \
        -v "${RAIZ}/evaluation:/data/evaluation:ro" \
        spark spark-submit /data/evaluation/oracle/full_compare.py \
          "/data/outputs${leaf#"${RUN_DIR}"/outputs}" "/data/gabaritos/${TARGET_TABLE}" \
          --key h3_cell --geom geometry \
          > "${oralog}" 2>&1)
      ec=$?
      [ ${ec} -eq 0 ] && echo "oraculo: MATCH (Medida 1 correta)" >> "${REPORT}" \
                      || echo "oraculo: MISMATCH (ver audit/oracle_${TARGET_TABLE}.txt)" >> "${REPORT}"
    fi
  else
    echo "oraculo: SKIP (gabarito ${TARGET_TABLE} ainda não existe — P8)" >> "${REPORT}"
  fi

  # 7b) MÉTRICAS ESTÁTICAS (Medida 2). Concatena os jobs gerados num único .py
  # (AST-friendly) e roda cada instrumento UMA vez -> cada item 2.* emitido 1x.
  # Reúso: 10 classes de equivalência no reuse_check. Schema: fonte única
  # (schema_source), sem --schema.
  JOBS=$(find "${RUN_DIR}/workdir/apps/educacao/jobs" -name '*.py' ! -name '__init__.py' 2>/dev/null)
  ALL_JOBS="${RUN_DIR}/audit/_all_jobs.py"
  if [ -n "${JOBS}" ]; then
    : > "${ALL_JOBS}"
    for job in ${JOBS}; do
      echo "# === ${job} ===" >> "${ALL_JOBS}"; cat "${job}" >> "${ALL_JOBS}"; echo "" >> "${ALL_JOBS}"
    done
    "${PYTHON_BIN:-python}" "${RAIZ}/evaluation/metrics/schema_fields.py" "${ALL_JOBS}" \
      > "${RUN_DIR}/audit/schema_fields.txt" 2>&1
    "${PYTHON_BIN:-python}" "${RAIZ}/evaluation/metrics/reuse_check.py" "${ALL_JOBS}" \
      --utils-dir "${RUN_DIR}/workdir/utils" > "${RUN_DIR}/audit/reuse_check.txt" 2>&1
    "${PYTHON_BIN:-python}" "${RAIZ}/evaluation/metrics/style_check.py" "${ALL_JOBS}" \
      > "${RUN_DIR}/audit/style_check.txt" 2>&1
    "${PYTHON_BIN:-python}" "${RAIZ}/evaluation/metrics/convention.py" \
      --jobs-dir "${RUN_DIR}/workdir/apps/educacao/jobs" \
      --outputs "${RUN_DIR}/outputs" \
      --metadata "${RUN_DIR}/workdir/apps/educacao/metadata.toml" \
      > "${RUN_DIR}/audit/convention.txt" 2>&1
    echo "metricas: schema/reuso/estilo/convencao -> audit/" >> "${REPORT}"
  else
    echo "metricas: nenhum job em apps/educacao/jobs — 2.* = FAIL (falha do agente)" >> "${REPORT}"
    { for k in 1 2; do echo "2.1.${k} ausente FAIL"; done
      for k in $(seq 1 12); do echo "2.2.${k} ausente FAIL"; done
      for k in $(seq 1 16); do echo "2.3.${k} ausente FAIL"; done
      for k in $(seq 1 4); do echo "2.4.${k} ausente FAIL"; done
    } > "${RUN_DIR}/audit/metrics_absent.txt"
  fi

  # 7c) monta o checklist.tsv (43 itens: 9 correção + 34 contexto) + gate de completude
  "${PYTHON_BIN:-python}" "${RAIZ}/evaluation/run/build_checklist.py" \
    --audit-dir "${RUN_DIR}/audit" --out "${RUN_DIR}/checklist.tsv" \
    > "${RUN_DIR}/audit/checklist_build.txt" 2>&1
  cl_ec=$?
  cat "${RUN_DIR}/audit/checklist_build.txt" >> "${REPORT}"
  [ ${cl_ec} -eq 0 ] || echo "checklist: GATE DE COMPLETUDE FALHOU (run inválida)" >> "${REPORT}"

  # auditoria de isolamento (acessos ⊆ workdir)
  if [ -d "${RUN_DIR}/audit/transcripts" ]; then
    "${PYTHON_BIN:-python}" "${RAIZ}/evaluation/audit/isolation_check.py" \
      "${RUN_DIR}/audit/transcripts" --workdir "${RUN_DIR}/workdir" \
      > "${RUN_DIR}/audit/isolation.txt" 2>&1 \
      && echo "isolamento: ok" >> "${REPORT}" || echo "isolamento: VER audit/isolation.txt" >> "${REPORT}"
  else
    echo "isolamento: SKIP (sem transcripts)" >> "${REPORT}"
  fi

  echo "fim:     $(date -Iseconds)" >> "${REPORT}"
  echo ">> relatório: ${REPORT}"
  cat "${REPORT}"
done

echo ""
echo "================ concluído: ${N_RUNS} run(s), ${FALHAS} falha(s) de infra ================"
