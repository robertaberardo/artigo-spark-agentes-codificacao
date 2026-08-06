"""Contrato entre o ORÁCULO e o montador do checklist.

`full_compare.py` roda dentro do container (pyspark/sedona) e `build_checklist.py`
roda no host: os dois só se entendem pelo FORMATO das linhas de saída. Se o
formato mudar de um lado, o montador para de enxergar itens/gates **em silêncio**
— e um item não visto vira `NAO_AVALIADO`, que invalida a execução.

Aqui o oráculo é importado com pyspark/sedona stubados (as funções de emissão só
usam `print`) e a saída dele é alimentada ao montador de verdade.
"""
import contextlib
import io
import os
import sys
import types

import build_checklist as bc
import schema_source as S
from conftest import REPO


def _load_full_compare():
    """Importa `full_compare` sem pyspark/sedona instalados (stubs em sys.modules)."""
    for name in ("pyspark", "pyspark.sql", "sedona", "sedona.spark"):
        sys.modules.setdefault(name, types.ModuleType(name))
    sys.modules["pyspark"].sql = sys.modules["pyspark.sql"]
    sys.modules["pyspark.sql"].functions = types.ModuleType("functions")
    sys.modules["sedona"].spark = sys.modules["sedona.spark"]
    sys.modules["sedona.spark"].SedonaContext = object
    sys.path.insert(0, os.path.join(REPO, "evaluation", "oracle"))
    import full_compare  # noqa: PLC0415

    return full_compare


def _emit(fn, *args):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        fn(*args)
    return buf.getvalue()


def _scan(tmp_path, text):
    audit = tmp_path / "audit"
    audit.mkdir()
    (audit / "oracle_educacao_escola_matricula_h3_grid.txt").write_text(text, encoding="utf-8")
    return bc.scan(str(audit))


def test_itens_do_oraculo_sao_lidos_pelo_montador(tmp_path):
    fc = _load_full_compare()
    verdicts = {iid: True for iid, _ in fc.ITEM_LABELS}
    found, _vars, gates, warns = _scan(tmp_path, _emit(fc.emit_checklist, verdicts, True, [], []))

    assert set(found) == {f"1.{k}" for k in range(1, 9)}
    assert all(v[0] == "PASS" for v in found.values())
    assert gates["schema_contrato"][0] == "PASS"
    assert warns == []


def test_gate_de_schema_reprovado_chega_ao_montador(tmp_path):
    fc = _load_full_compare()
    verdicts = {iid: True for iid, _ in fc.ITEM_LABELS}
    text = _emit(fc.emit_checklist, verdicts, False, ["avg_enrollment"], [])
    _found, _vars, gates, _warns = _scan(tmp_path, text)

    assert gates["schema_contrato"][0] == "FAIL"
    assert "avg_enrollment" in gates["schema_contrato"][1]


def test_avisos_sao_lidos_e_nao_viram_item(tmp_path):
    fc = _load_full_compare()
    text = _emit(fc.warn, "schema_extras", "colunas não contratadas: ['lixo']")
    found, _vars, gates, warns = _scan(tmp_path, text)

    assert found == {} and gates == {}
    assert [w[0] for w in warns] == ["schema_extras"]
    assert "lixo" in warns[0][1]


def test_rotulos_do_oraculo_seguem_a_fonte_unica():
    """P15: renomear coluna em `schema_source` não pode deixar o oráculo para trás."""
    fc = _load_full_compare()
    labels = [label for _iid, label in fc.ITEM_LABELS]
    contratadas = S.target_column_names()

    assert len(fc.ITEM_LABELS) == 8
    assert labels[:7] == contratadas[:7]          # 1.1–1.7 = as 7 colunas de dados
    assert labels[7] == S.GEOMETRY_COLUMN          # 1.8 = conteúdo da geometria
    # O CRS de armazenamento NÃO é item pontuado: é variável registrada (VAR crs).
    assert not any("crs" in lb.lower() for lb in labels)
