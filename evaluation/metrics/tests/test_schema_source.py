"""P15 — a fonte única e o metadata do overlay C2/C5 não podem divergir."""
import os
import tomllib

import schema_source as S
from conftest import REPO

C2 = os.path.join(REPO, "overlays", "C2-schema-metadata", "apps", "educacao", "metadata.toml")
C5 = os.path.join(REPO, "overlays", "C5-final", "apps", "educacao", "metadata.toml")


def _load(p):
    return tomllib.load(open(p, "rb"))


def test_c2_c5_byte_identical():
    assert open(C2, "rb").read() == open(C5, "rb").read()


def test_app_and_db_match_source():
    meta = _load(C2)
    assert meta["data_lake"]["app"] == S.APP
    assert meta["data_lake"]["db"] == S.DB


def test_source_tables_match_metadata():
    meta = _load(C2)
    for table, cols in S.SOURCE_SCHEMAS.items():
        entry = meta["tables"][table]
        assert entry["name"] == f"{S.APP}_{table}", entry
        meta_fields = [c["name"] for c in entry["raw"]["schema"]]
        src_fields = [c[0] for c in cols]
        assert meta_fields == src_fields, (table, meta_fields, src_fields)


def test_documented_tables_match_both_ways():
    """P15 nos dois sentidos: tabela com schema no metadata TEM de estar na fonte.

    Sem isto, uma tabela documentada só no `metadata.toml` (era o caso de
    `turma`) passa despercebida e seus campos contam como alucinação em 2.1.
    """
    meta = _load(C2)
    documented = {t for t, e in meta["tables"].items() if "raw" in e}
    undocumented = {t for t, e in meta["tables"].items() if "raw" not in e}
    assert documented == set(S.SOURCE_SCHEMAS), (documented, set(S.SOURCE_SCHEMAS))
    assert undocumented == set(S.UNDOCUMENTED_SOURCE_TABLES)


def test_semantics_documented_in_metadata():
    # os códigos (federal=1, em atividade=1) aparecem na descrição do metadata
    meta = _load(C2)
    escola = {c["name"]: c.get("description", "") for c in meta["tables"]["escola"]["raw"]["schema"]}
    assert "1 federal" in escola["SETOR"]
    assert "1 em atividade" in escola["SITUACAO"]


def test_target_contract_shape():
    assert S.TARGET_TABLE == "educacao_escola_matricula_h3_grid"
    assert [c[0] for c in S.TARGET_SCHEMA] == [
        "h3_cell", "n_schools", "total_enrollment", "avg_enrollment",
        "n_federal_neighbors", "avg_dist_neighbors_km", "avg_dist_nearest_km", "geometry",
    ]
    assert "avg_dist_neighbors_km" in S.NULLABLE_COLUMNS
