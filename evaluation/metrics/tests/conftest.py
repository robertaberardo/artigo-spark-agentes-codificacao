"""Coloca `evaluation/` e `evaluation/metrics/` no path para os testes."""
import os
import sys

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..")))  # evaluation/
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..")))        # evaluation/metrics/
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..", "run")))  # evaluation/run/

# raiz do repo (para achar repo-baseline/utils)
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
UTILS_DIR = os.path.join(REPO, "repo-baseline", "utils")


def write(tmp_path, name, code):
    p = tmp_path / name
    p.write_text(code, encoding="utf-8")
    return str(p)
