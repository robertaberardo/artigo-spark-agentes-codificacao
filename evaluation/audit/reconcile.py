"""Reconciliação de uma execução: junta hooks-log + transcript(s) num registro
completo, e sinaliza divergências/truncamentos.

- Fonte PRIMÁRIA de completude: ``hooks.jsonl`` (I/O completo capturado no momento
  da chamada pelos hooks PostToolUse/UserPromptSubmit).
- Fonte SECUNDÁRIA: os transcripts ``<sessionId>.jsonl`` (+ de subagentes), de onde
  vêm os textos/raciocínio do assistente.

Não altera os originais: escreve ``reconciled.json`` na pasta de auditoria da run.

Uso:
    python reconcile.py <run_audit_dir>
        run_audit_dir contém hooks.jsonl e/ou transcripts/
"""
from __future__ import annotations

import json
import os
import sys


def raw_decode_stream(text):
    """Percorre um fluxo de objetos JSON concatenados (o formato do hooks.jsonl)."""
    dec = json.JSONDecoder()
    i, n = 0, len(text)
    while i < n:
        while i < n and text[i] in " \t\r\n":
            i += 1
        if i >= n:
            break
        try:
            obj, end = dec.raw_decode(text, i)
        except json.JSONDecodeError:
            i += 1
            continue
        yield obj
        i = end


def load_hooks(path):
    """Lê o hooks.jsonl e separa prompts completos e chamadas de ferramenta."""
    prompts, tool_calls = [], []
    if not os.path.isfile(path):
        return prompts, tool_calls
    with open(path, encoding="utf-8", errors="replace") as fh:
        for obj in raw_decode_stream(fh.read()):
            ev = obj.get("hook_event_name") or obj.get("hook_event")
            if ev == "UserPromptSubmit":
                prompts.append(obj.get("prompt", ""))
            elif ev == "PostToolUse":
                tool_calls.append(
                    {
                        "tool": obj.get("tool_name"),
                        "input": obj.get("tool_input"),
                        "response": obj.get("tool_response"),
                    }
                )
    return prompts, tool_calls


def iter_jsonl(path):
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def load_transcripts(directory):
    """Extrai textos do assistente, tool_use e tool_result dos transcripts."""
    texts, uses, results = [], [], []
    if not os.path.isdir(directory):
        return texts, uses, results
    for root, _dirs, files in os.walk(directory):
        for f in files:
            if not f.endswith(".jsonl"):
                continue
            for obj in iter_jsonl(os.path.join(root, f)):
                msg = obj.get("message") or {}
                content = msg.get("content")
                if not isinstance(content, list):
                    continue
                for block in content:
                    if not isinstance(block, dict):
                        continue
                    btype = block.get("type")
                    if btype == "text":
                        texts.append(block.get("text", ""))
                    elif btype == "tool_use":
                        uses.append({"tool": block.get("name"), "input": block.get("input")})
                    elif btype == "tool_result":
                        c = block.get("content")
                        length = len(json.dumps(c)) if c is not None else 0
                        results.append({"length": length, "truncated_flag": "truncated" in json.dumps(c).lower() if c else False})
    return texts, uses, results


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    run_dir = sys.argv[1]
    hooks_path = os.path.join(run_dir, "hooks.jsonl")
    transcripts_dir = os.path.join(run_dir, "transcripts")

    prompts, hook_calls = load_hooks(hooks_path)
    texts, uses, results = load_transcripts(transcripts_dir)

    # Divergência simples: nº de chamadas capturadas por hooks vs transcript.
    divergence = abs(len(hook_calls) - len(uses))

    reconciled = {
        "source": {
            "hooks_present": os.path.isfile(hooks_path),
            "transcripts_present": os.path.isdir(transcripts_dir),
        },
        "prompts_complete": prompts,           # completos (hooks)
        "tool_calls_complete": hook_calls,     # I/O completo (hooks)
        "assistant_texts": texts,              # do transcript
        "counts": {
            "prompts": len(prompts),
            "tool_calls_hooks": len(hook_calls),
            "tool_uses_transcript": len(uses),
            "tool_results_transcript": len(results),
            "assistant_texts": len(texts),
            "divergence_hooks_vs_transcript": divergence,
        },
    }

    out = os.path.join(run_dir, "reconciled.json")
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(reconciled, fh, ensure_ascii=False, indent=2)

    print("=== reconciliação ===")
    for k, v in reconciled["counts"].items():
        print(f"  {k:34s} {v}")
    print(f"  hooks_present={reconciled['source']['hooks_present']} "
          f"transcripts_present={reconciled['source']['transcripts_present']}")
    if not reconciled["source"]["hooks_present"]:
        print("  AVISO: sem hooks.jsonl — completude limitada ao que o transcript salvou.")
    print(f"  -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
