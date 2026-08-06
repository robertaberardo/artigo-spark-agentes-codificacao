"""Reúso por classes de equivalência: dez classes, combinação sobre o corpo inteiro."""
from conftest import UTILS_DIR, write

import reuse_check as rc

GOOD = '''
from utils import column_transforms as ct
from utils import dataframe_transforms as dt

def main():
    """reutiliza utils."""
    a = ct.to_coordinate(col)
    b = ct.blank_to_null(col)
    c = ct.normalize_text(col)
    d = dt.join_no_fanout(x, y, on="k")
    e = dt.h3_join(g)
'''

REIMPL = '''
def to_coordinate(x):
    """reimplementa em vez de reutilizar."""
    return x.cast("double")

def main():
    """usa a versão local."""
    a = to_coordinate(col)
'''

# Membro alternativo da classe de reprojeção: exercita a equivalência funcional.
ALTERNATIVO = '''
from utils import dataframe_transforms as dt

def main():
    """reprojeta pelo helper específico, não pelo genérico."""
    return dt.to_web_mercator(df)
'''

# Segundo arquivo do corpo: exerce algo que o primeiro não exerce.
PROMOVIDO = '''
from utils import column_transforms as ct

def add_kring(df):
    """função que a execução promoveu para utils/."""
    return ct.zero_pad_code(df)
'''


def _por_classe(caminhos):
    resultados, _dedupe = rc.evaluate(caminhos, UTILS_DIR)
    return {classe: ok for _i, classe, ok, _ev, _reg in resultados}


def test_reuso_por_classe_contado(tmp_path):
    res = _por_classe([write(tmp_path, "good.py", GOOD)])
    assert res["to_coordinate"] and res["blank_to_null"]
    assert res["join_no_fanout"] and res["grade_h3"]


def test_reimplementacao_nao_conta(tmp_path):
    res = _por_classe([write(tmp_path, "reimpl.py", REIMPL)])
    assert res["to_coordinate"] is False


def test_classe_passa_por_qualquer_membro(tmp_path):
    """`to_web_mercator` sozinho exerce a classe de reprojeção.

    É o construto que as classes preservam: escolher entre utilitários equivalentes não
    é ausência de reúso.
    """
    res = _por_classe([write(tmp_path, "alt.py", ALTERNATIVO)])
    assert res["reprojecao"] is True


def test_corpo_combina_arquivos(tmp_path):
    """Vários arquivos formam UM corpo, e o reúso é a união do que cada um exerce.

    Regressão do erro silencioso: emitir um bloco por arquivo faria o montador do
    checklist descartar tudo depois do primeiro.
    """
    a = write(tmp_path, "jobs.py", GOOD)
    b = write(tmp_path, "promovido.py", PROMOVIDO)
    so_jobs = _por_classe([a])
    corpo = _por_classe([a, b])
    assert so_jobs["zero_pad_code"] is False
    assert corpo["zero_pad_code"] is True
    assert corpo["grade_h3"] is True  # o que veio do primeiro arquivo não se perde


def test_denominador_de_dez_classes():
    assert len(rc.CLASSES) == 10


def test_deduplicacao_fora_do_denominador_e_reportada(tmp_path):
    """`deduplicate_by_key` não pontua, mas continua observada."""
    classes = [c for c, _m, _r in rc.CLASSES]
    assert "deduplicate_by_key" not in classes
    codigo = ('from utils import dataframe_transforms as dt\n\n'
              'def main():\n    """usa dedupe."""\n    return dt.deduplicate_by_key(df, ["k"])\n')
    _res, dedupe = rc.evaluate([write(tmp_path, "d.py", codigo)], UTILS_DIR)
    assert dedupe is not None


def test_identificador_registrado_acompanha_cada_classe():
    """Sem o identificador registrado, o crosswalk teria de ser montado à mão.

    Os identificadores NÃO são estáveis entre as composições: a partir de `2.2.6` um
    mesmo número designa itens diferentes.
    """
    registrados = {c: r for c, _m, r in rc.CLASSES}
    assert registrados["filter_valid_coordinates"] == "2.2.7"
    assert registrados["reprojecao"] == "2.2.10+2.2.11"
