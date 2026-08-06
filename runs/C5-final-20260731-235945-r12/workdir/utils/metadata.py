from __future__ import annotations

import os
import tomllib
from pathlib import Path

APPS_DIR = Path(__file__).resolve().parent.parent / "apps"


def load_metadata(app: str) -> dict:
    """Carrega o ``metadata.toml`` da app ``app`` (em ``apps/<app>/``)."""
    with open(APPS_DIR / app / "metadata.toml", "rb") as f:
        return tomllib.load(f)


def table_location(
    app: str, table: str, level: str = "raw", *, meta: dict | None = None
) -> str:
    """Devolve o caminho S3A da tabela na camada pedida, lido do metadata.

    Caminho: ``s3a://{db}/{app}/{name}/{level}``. O bucket vem do metadata
    (``data_lake.db``), com override por ``S3_BUCKET`` para paridade com os jobs
    atuais e com o cluster de produção (ver ``prod-cluster-context``).

    Levanta ``KeyError`` se a tabela não está declarada e ``ValueError`` se a
    camada pedida ainda não foi ingerida — falha cedo, em vez de o job ler um
    prefixo vazio.
    """
    meta = meta or load_metadata(app)
    db = os.environ.get("S3_BUCKET", meta["data_lake"]["db"])
    app_name = meta["data_lake"]["app"]

    tables = meta.get("tables", {})
    if table not in tables:
        raise KeyError(f"tabela '{table}' não declarada no metadata de '{app}'")
    entry = tables[table]
    if level not in entry.get("levels", []):
        raise ValueError(
            f"level '{level}' não disponível para '{table}'; "
            f"declarados: {entry.get('levels', [])}"
        )
    return f"s3a://{db}/{app_name}/{entry['name']}/{level}"
