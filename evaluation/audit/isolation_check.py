"""Auditoria de isolamento de uma sessão do Claude Code.

Lê o(s) transcript(s) `<sessionId>.jsonl` (e de subagentes) e verifica, a partir
das chamadas de ferramenta (`tool_use`), que nenhum acesso de ARQUIVO saiu do
diretório de trabalho permitido. Chamadas de rede (web/MCP) são internet liberada
por decisão do projeto: são REPORTADAS como variável observada, não reprovam.

Também extrai dos eventos `attachment` o que de fato ficou disponível na sessão
(ferramentas deferidas, agentes) — prova empírica do que carregou.

Detecta ainda o "piso irredutível da OSPA": conectores MCP do time (nomes com
`OSPA`) e as skills geridas (`tom-e-regras`/`humanizer`). Disponibilidade é
observada; INVOCAÇÃO conta como contaminação da run.

Uso:
    python isolation_check.py <arquivo_ou_dir_de_transcripts> [--workdir DIR]

Saída: relatório + código de saída = violações de arquivo + invocações OSPA (0 = ok).
"""
from __future__ import annotations

import json
import os
import sys

FILE_TOOLS = {"Read", "Write", "Edit", "NotebookEdit"}
SEARCH_TOOLS = {"Glob", "Grep", "LS"}
WEB_TOOLS = {"WebFetch", "WebSearch"}

# Conectores e skills de organização removidos na fonte (ver README.md, Isolamento);
# esta guarda permanece: uma run limpa NÃO deve INVOCÁ-lo. Disponibilidade é observada;
# invocação é sinalizada como contaminação e entra no código de saída.
OSPA_TOOL_MARKERS = ("ospa",)  # casa mcp__..._CRM_OSPA__*, _OSPA_Capital__*, etc. (case-insensitive)
OSPA_SKILLS = {"tom-e-regras", "humanizer"}


def iter_lines(path):
    """Gera objetos JSON de um arquivo .jsonl, tolerando linhas inválidas."""
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def collect_transcripts(target):
    """Retorna a lista de arquivos de transcript (inclui subagentes)."""
    if os.path.isfile(target):
        return [target]
    found = []
    for root, _dirs, files in os.walk(target):
        for f in files:
            if f.endswith(".jsonl"):
                found.append(os.path.join(root, f))
    return found


def norm(path):
    return os.path.normcase(os.path.normpath(os.path.abspath(path)))


def is_inside(path, workdir):
    try:
        return os.path.commonpath([norm(path), norm(workdir)]) == norm(workdir)
    except ValueError:
        return False  # drives diferentes (Windows)


def extract_tool_uses(obj):
    """Extrai (nome, input) dos blocos tool_use de uma mensagem assistant."""
    msg = obj.get("message") or {}
    content = msg.get("content")
    if not isinstance(content, list):
        return
    for block in content:
        if isinstance(block, dict) and block.get("type") == "tool_use":
            yield block.get("name", "?"), block.get("input") or {}


def paths_from(tool, inp):
    """Devolve os caminhos de arquivo referenciados por uma chamada de ferramenta."""
    out = []
    if tool in FILE_TOOLS:
        if inp.get("file_path"):
            out.append(inp["file_path"])
        if inp.get("notebook_path"):
            out.append(inp["notebook_path"])
    elif tool in SEARCH_TOOLS:
        if inp.get("path"):
            out.append(inp["path"])
    return out


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    target = sys.argv[1]
    if "--workdir" in sys.argv:
        workdir = sys.argv[sys.argv.index("--workdir") + 1]
    else:
        # getcwd() pode falhar (cwd apagado/atravessando WSL); só é necessário
        # quando --workdir não é informado.
        try:
            workdir = os.getcwd()
        except OSError:
            print("!! cwd indisponível — informe --workdir explicitamente")
            return 2

    transcripts = collect_transcripts(target)
    if not transcripts:
        print(f"!! nenhum transcript encontrado em {target}")
        return 2

    file_accesses = 0
    violations = []
    web_calls = []
    bash_cmds = 0
    available_tools = set()
    available_agents = set()
    ospa_called = []       # invocações de conectores MCP da OSPA (contaminação)
    skills_called = []      # todas as skills invocadas (via ferramenta Skill)

    for tpath in transcripts:
        for obj in iter_lines(tpath):
            att = obj.get("attachment") or {}
            if att.get("type") == "deferred_tools_delta":
                available_tools.update(att.get("addedNames", []) or [])
            if att.get("type") == "agent_listing_delta":
                available_agents.update(att.get("addedTypes", []) or [])

            if obj.get("type") != "assistant":
                continue
            for tool, inp in extract_tool_uses(obj):
                if tool in WEB_TOOLS:
                    web_calls.append((tool, inp.get("url") or inp.get("query") or ""))
                elif tool == "Bash":
                    bash_cmds += 1
                if any(m in tool.lower() for m in OSPA_TOOL_MARKERS):
                    ospa_called.append((tool, inp.get("url") or inp.get("query") or ""))
                if tool == "Skill":
                    sk = (inp.get("skill") or "").strip()
                    if sk:
                        skills_called.append(sk)
                for p in paths_from(tool, inp):
                    file_accesses += 1
                    if os.path.isabs(p) and not is_inside(p, workdir):
                        violations.append((tool, p))

    mcp_tools = sorted(t for t in available_tools if t.startswith("mcp__"))
    ospa_available = sorted(
        t for t in available_tools if any(m in t.lower() for m in OSPA_TOOL_MARKERS)
    )
    ospa_skills_called = sorted(s for s in set(skills_called) if s in OSPA_SKILLS)
    contaminacao = len(ospa_called) + len(ospa_skills_called)

    print("=== auditoria de isolamento ===")
    print(f"workdir permitido : {norm(workdir)}")
    print(f"transcripts       : {len(transcripts)}")
    print(f"acessos a arquivo : {file_accesses}")
    print(f"comandos Bash     : {bash_cmds} (checar caminhos manualmente se preciso)")
    print(f"chamadas web      : {len(web_calls)} (internet liberada -> observada)")
    for tool, ref in web_calls[:20]:
        print(f"    web: {tool} {ref}")
    print(f"MCP disponiveis   : {len(mcp_tools)}")
    if mcp_tools:
        print("    " + ", ".join(mcp_tools[:15]) + (" ..." if len(mcp_tools) > 15 else ""))
    print(f"agentes disponiveis: {sorted(available_agents)}")

    print("\n=== piso OSPA ===")
    print(f"conectores OSPA disponiveis (observado): {len(ospa_available)}")
    if ospa_available:
        print("    " + ", ".join(ospa_available[:15]) + (" ..." if len(ospa_available) > 15 else ""))
    print(f"skills invocadas                       : {sorted(set(skills_called))}")
    print(f"!! conectores OSPA INVOCADOS           : {len(ospa_called)}")
    for tool, ref in ospa_called[:20]:
        print(f"    OSPA-call: {tool} {ref}")
    print(f"!! skills OSPA INVOCADAS               : {ospa_skills_called}")

    print(f"\n=== VIOLACOES de arquivo (fora do workdir): {len(violations)} ===")
    for tool, p in violations[:50]:
        print(f"    {tool}: {p}")
    if not violations:
        print("    nenhuma — todos os acessos de arquivo dentro do workdir.")

    print(f"\n=== CONTAMINACAO OSPA (invocacoes): {contaminacao} ===")
    if contaminacao == 0:
        print("    nenhuma — nenhum conector/skill da OSPA foi invocado.")

    return len(violations) + contaminacao


if __name__ == "__main__":
    sys.exit(main())
