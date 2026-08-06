"""Reconstrói o transcript de uma run (Claude Code .jsonl) como um .md tipo chat.

Lê o transcript da sessão do agente e escreve um Markdown que se lê como a
conversa inteira: prompt do usuário, respostas do assistente, e cada tool call
(input) com seu resultado (resumido) em blocos <details> recolhíveis.

O raciocínio (thinking) é persistido: cada bloco traz o texto no campo `thinking`
(ao lado da assinatura criptografada, que não vai para o relatório). Por isso ele
sai como <details> recolhível, igual aos tool calls. Blocos sem conteúdo aparecem
(thinking redigido) e caem num marcador de contagem.

Uso:
    python transcript_to_md.py <transcript.jsonl> <saida.md>

O transcript de uma run costuma estar em:
    <RUN_DIR>/claude-home/projects/<slug-do-workdir>/<uuid>.jsonl
"""
import json
import sys

CAP = 1500  # corte de inputs/resultados longos (caracteres)
THINK_CAP = 6000  # raciocínio: corte mais folgado, senão perde o essencial


def blocks(msg):
    c = msg.get("content")
    if isinstance(c, str):
        return [{"type": "text", "text": c}]
    return c if isinstance(c, list) else []


def clip(s, n=CAP):
    s = str(s)
    return s if len(s) <= n else s[:n] + "\n… (truncado)"


def render(src, out):
    lines = ["# Transcript — sessão do agente\n"]
    seen_first_user = False
    last = None  # agrupa mensagens consecutivas do assistente sob um só cabeçalho

    with open(src, encoding="utf-8") as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                ev = json.loads(raw)
            except json.JSONDecodeError:
                continue
            msg = ev.get("message")
            if not isinstance(msg, dict):
                continue
            role = msg.get("role")
            if role not in ("user", "assistant"):
                continue

            bs = blocks(msg)
            texts = [b for b in bs if isinstance(b, dict) and b.get("type") == "text" and b.get("text", "").strip()]
            tool_uses = [b for b in bs if isinstance(b, dict) and b.get("type") == "tool_use"]
            tool_res = [b for b in bs if isinstance(b, dict) and b.get("type") == "tool_result"]
            thinking = [b for b in bs if isinstance(b, dict) and b.get("type") == "thinking"]

            if role == "user" and texts and not tool_res:
                body = "\n".join(t["text"].strip() for t in texts)
                header = "## Você (prompt inicial)" if not seen_first_user else "## Você"
                seen_first_user = True
                lines.append(f"{header}\n\n{clip(body, 4000)}\n")
                last = "user"
            elif role == "assistant":
                chunk = []
                thought = [b.get("thinking", "").strip() for b in thinking]
                for th in [t for t in thought if t]:
                    chunk.append(
                        f"<details><summary>raciocínio</summary>\n\n"
                        f"{clip(th, THINK_CAP)}\n</details>"
                    )
                vazios = sum(1 for t in thought if not t)
                if vazios:
                    chunk.append(f"_[{vazios} bloco(s) de raciocínio sem conteúdo persistido]_")
                for t in texts:
                    chunk.append(t["text"].strip())
                for tu in tool_uses:
                    inp = json.dumps(tu.get("input", {}), ensure_ascii=False, indent=2)
                    chunk.append(
                        f"<details><summary><code>{tu.get('name')}</code></summary>\n\n"
                        f"```json\n{clip(inp)}\n```\n</details>"
                    )
                if chunk:
                    if last != "assistant":
                        lines.append("## Claude\n")
                    lines.append("\n".join(chunk) + "\n")
                    last = "assistant"
            elif role == "user" and tool_res:
                for tr in tool_res:
                    c = tr.get("content", "")
                    if isinstance(c, list):
                        c = " ".join(x.get("text", "") for x in c if isinstance(x, dict))
                    c = c.strip()
                    if c:
                        lines.append(
                            f"<details><summary>resultado</summary>\n\n```\n{clip(c)}\n```\n</details>\n"
                        )

    with open(out, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    print(f"escrito: {out}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("uso: python transcript_to_md.py <transcript.jsonl> <saida.md>")
    render(sys.argv[1], sys.argv[2])
