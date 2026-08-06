"""Fonte ÚNICA de schema do experimento (P15).

Uma só definição, aqui, alimenta:
  * o `metadata.toml` do overlay C2/C5 (schema + semântica) — conferido pelo
    teste `tests/test_schema_source.py::test_metadata_consistente`;
  * a tipagem/arredondamento do gabarito (`evaluation/gabaritos/*`);
  * as expectativas do oráculo (`evaluation/oracle/full_compare.py`);
  * a allowlist de campos-fonte da métrica de schema (`metrics/schema_fields.py`);
  * o ofuscamento do raw (`RAW_RENAME`, aplicado por `storage/scripts/debrand_censo.py`).

Divergência entre esses artefatos fica impossível: todos importam daqui.

Nada de segredo do gabarito mora neste módulo (contagens, caminhos, memória);
só o CONTRATO (nomes/tipos/semântica dos códigos), que já é público no prompt e
no metadata do C2.
"""
from __future__ import annotations

DB = "datalake"
APP = "educacao"

# --- Ofuscação (D-3): coluna real da base -> nome de-brandado no raw semeado ----
# Os nomes ORIGINAIS (impressão-digital que o agente reconstruía do prior) só
# aparecem aqui, como CHAVE. O `debrand_censo.py` projeta+renomeia o raw por este
# mapa; do lake para baixo só existem os nomes de-brandados. As colunas de SAÍDA
# (`TARGET_SCHEMA`) já são de-brandadas — aqui é só a ENTRADA.
RAW_RENAME: dict[str, dict[str, str]] = {
    "escola": {
        "CO_ENTIDADE": "ID_UNIDADE", "NO_ENTIDADE": "NOME_UNIDADE",
        "SG_UF": "UF", "CO_UF": "COD_UF",
        "NO_MUNICIPIO": "NOME_MUNICIPIO", "CO_MUNICIPIO": "COD_MUNICIPIO",
        "TP_DEPENDENCIA": "SETOR", "TP_LOCALIZACAO": "AREA",
        "TP_SITUACAO_FUNCIONAMENTO": "SITUACAO",
        "LATITUDE": "LATITUDE", "LONGITUDE": "LONGITUDE",
        "QT_SALAS_UTILIZADAS": "QT_SALAS",
        "IN_AGUA_POTAVEL": "TEM_AGUA", "IN_ENERGIA_REDE_PUBLICA": "TEM_ENERGIA",
        "IN_ESGOTO_REDE_PUBLICA": "TEM_ESGOTO", "IN_BANHEIRO": "TEM_BANHEIRO",
        "IN_BIBLIOTECA": "TEM_BIBLIOTECA", "IN_LABORATORIO_INFORMATICA": "TEM_LAB_INFO",
        "IN_QUADRA_ESPORTES": "TEM_QUADRA", "IN_INTERNET": "TEM_INTERNET",
    },
    "matricula": {
        "CO_ENTIDADE": "ID_UNIDADE", "QT_MAT_BAS": "MAT_BASICA",
        "QT_MAT_INF": "MAT_INFANTIL", "QT_MAT_FUND": "MAT_FUNDAMENTAL",
        "QT_MAT_MED": "MAT_MEDIO", "QT_MAT_PROF": "MAT_PROFISSIONAL",
        "QT_MAT_EJA": "MAT_EJA", "QT_MAT_ESP": "MAT_ESPECIAL",
    },
    "turma": {"CO_ENTIDADE": "ID_UNIDADE", "QT_TUR_BAS": "TUR_BASICA"},
}

# Colunas de origem CRÍTICAS: o debrand aborta se faltarem no header real; o ruído
# (IN_*/QT_SALAS) ausente é só avisado e sai do schema (não vira alucinação em 2.1).
CRITICAL_RAW: dict[str, list[str]] = {
    "escola": ["CO_ENTIDADE", "TP_DEPENDENCIA", "TP_SITUACAO_FUNCIONAMENTO", "LATITUDE", "LONGITUDE"],
    "matricula": ["CO_ENTIDADE", "QT_MAT_BAS", "QT_MAT_INF", "QT_MAT_FUND",
                  "QT_MAT_MED", "QT_MAT_PROF", "QT_MAT_EJA", "QT_MAT_ESP"],
    "turma": ["CO_ENTIDADE", "QT_TUR_BAS"],
}

# --- Tabelas-fonte contratadas (camada raw, tudo string como aterrissa) --------
# Nomes JÁ de-brandados (o que existe no lake). `JOIN_KEY` = coluna de junção.
JOIN_KEY = "ID_UNIDADE"

SOURCE_SCHEMAS: dict[str, list[tuple[str, str, str]]] = {
    "escola": [
        ("ID_UNIDADE", "string", "código da unidade escolar (chave, 8 dígitos)"),
        ("NOME_UNIDADE", "string", "nome da unidade"),
        ("UF", "string", "sigla da unidade federativa"),
        ("COD_UF", "string", "código IBGE da UF"),
        ("NOME_MUNICIPIO", "string", "nome do município"),
        ("COD_MUNICIPIO", "string", "código IBGE do município"),
        ("SETOR", "string", "setor administrativo (1 federal, 2 estadual, 3 municipal, 4 privada)"),
        ("AREA", "string", "área (1 urbana, 2 rural)"),
        ("SITUACAO", "string", "situação de funcionamento (1 em atividade, 2 paralisada, 3 extinta)"),
        ("LATITUDE", "string", "latitude em graus decimais (texto, como aterrissa)"),
        ("LONGITUDE", "string", "longitude em graus decimais (texto, como aterrissa)"),
        ("QT_SALAS", "string", "quantidade de salas de aula utilizadas"),
        ("TEM_AGUA", "string", "indicador (0/1) de fornecimento de água potável"),
        ("TEM_ENERGIA", "string", "indicador (0/1) de energia elétrica da rede pública"),
        ("TEM_ESGOTO", "string", "indicador (0/1) de esgotamento sanitário da rede pública"),
        ("TEM_BANHEIRO", "string", "indicador (0/1) de banheiro"),
        ("TEM_BIBLIOTECA", "string", "indicador (0/1) de biblioteca"),
        ("TEM_LAB_INFO", "string", "indicador (0/1) de laboratório de informática"),
        ("TEM_QUADRA", "string", "indicador (0/1) de quadra de esportes"),
        ("TEM_INTERNET", "string", "indicador (0/1) de acesso à internet"),
    ],
    "matricula": [
        ("ID_UNIDADE", "string", "código da unidade escolar (chave)"),
        ("MAT_BASICA", "string", "matrículas na educação básica (total)"),
        ("MAT_INFANTIL", "string", "matrículas na educação infantil"),
        ("MAT_FUNDAMENTAL", "string", "matrículas no ensino fundamental"),
        ("MAT_MEDIO", "string", "matrículas no ensino médio"),
        ("MAT_PROFISSIONAL", "string", "matrículas na educação profissional"),
        ("MAT_EJA", "string", "matrículas na educação de jovens e adultos"),
        ("MAT_ESPECIAL", "string", "matrículas na educação especial"),
    ],
    # `turma` não entra na tarefa, mas está documentada no metadata do C2 — logo
    # entra aqui também, senão `TUR_BASICA` contaria como campo alucinado (2.1).
    "turma": [
        ("ID_UNIDADE", "string", "código da unidade escolar (chave)"),
        ("TUR_BASICA", "string", "turmas da educação básica (total)"),
    ],
}

# Sem tabelas-ruído: o de-brand projeta o lake para escola/matricula/turma (as
# outras teriam header do Censo, então saem da semeadura — ver load_to_minio.sh).
UNDOCUMENTED_SOURCE_TABLES: list[str] = []

# --- Semântica dos códigos (nunca traduzida no dado; só o contrato explicita) --
# Chaveada pelos nomes DE-BRANDADOS das colunas de código.
SEMANTICS = {
    "SETOR": {"federal": "1", "estadual": "2", "municipal": "3", "privada": "4"},
    "SITUACAO": {"em_atividade": "1", "paralisada": "2", "extinta": "3"},
}

# --- Tabela-alvo (gold) --------------------------------------------------------
TARGET_TABLE = "educacao_escola_matricula_h3_grid"
TARGET_LEVEL = "gold"
TARGET_CRS = "EPSG:3857"
H3_RESOLUTION = 5

# (coluna, tipo lógico, definição). Ordem = ordem contratada no prompt.
TARGET_SCHEMA: list[tuple[str, str, str]] = [
    ("h3_cell", "long", "célula H3 res 5 (chave)"),
    ("n_schools", "long", "nº de federais em atividade na célula"),
    ("total_enrollment", "long", "soma de MAT_BASICA (o total pré-agregado)"),
    ("avg_enrollment", "double", "média de MAT_BASICA por escola federal"),
    ("n_federal_neighbors", "long", "nº de OUTRAS federais no k-ring k=1 (7 células); constante na célula; 0=isolada"),
    ("avg_dist_neighbors_km", "double", "média (na célula) da distância geodésica de cada escola às suas vizinhas do anel; NULL se sem vizinhas"),
    ("avg_dist_nearest_km", "double", "média (na célula) da distância geodésica de cada escola à federal ativa mais próxima (mínimo global por escola)"),
    ("geometry", "geometry", "polígono da célula, EPSG:3857"),
]

# A precisão de saída NÃO é contrato deste módulo. A constante `FLOAT_COLUMNS`, que
# declarava seis casas decimais e nunca foi consumida em lugar nenhum, foi removida em
# 2026-08-03: era código morto que contradizia o valor efetivamente executado. A régua
# vigente é adaptativa — a tolerância deriva das casas decimais que o candidato
# gravou —, e por isso não há número de casas a declarar aqui.
# Ver `docs/ESPECIFICACAO.md §4.3`.

# Colunas que admitem NULL legítimo (nulo≠0): a distância média do anel vazio.
NULLABLE_COLUMNS = {"avg_dist_neighbors_km"}
KEY_COLUMNS = ["h3_cell"]
GEOMETRY_COLUMN = "geometry"


def source_field_names(table: str | None = None) -> set[str]:
    """Nomes de campos-fonte válidos (UPPER, como no raw). Sem tabela: união."""
    if table is not None:
        return {c[0] for c in SOURCE_SCHEMAS[table]}
    return {c[0] for t in SOURCE_SCHEMAS.values() for c in t}


def target_column_names() -> list[str]:
    return [c[0] for c in TARGET_SCHEMA]


def field_to_tables() -> dict[str, list[str]]:
    """Mapa campo-fonte -> tabelas onde existe (para conferência por tabela)."""
    out: dict[str, list[str]] = {}
    for table, cols in SOURCE_SCHEMAS.items():
        for name, _t, _d in cols:
            out.setdefault(name, []).append(table)
    return out
