from conftest import write

import schema_fields as sf

GOOD = '''
from pyspark.sql import functions as F

def main():
    """produz a gold."""
    esc = spark.read.schema(SCH).csv(p)
    mat = spark.read.schema(SCH2).csv(q)
    esc = esc.select(
        F.col("ID_UNIDADE"), F.col("SETOR"),
        F.col("SITUACAO"), F.col("LATITUDE"), F.col("LONGITUDE"),
    )
    mat = mat.select(F.col("ID_UNIDADE"), F.col("MAT_BASICA"))
    out = esc.join(mat, on="ID_UNIDADE", how="inner")
    out = out.withColumn("n_schools", F.lit(1)).withColumn("total_enrollment", F.lit(2))
'''

BAD = '''
from pyspark.sql import functions as F

def main():
    """alucina colunas inexistentes."""
    esc = spark.read.schema(SCH).csv(p)
    esc.select(F.col("ID_UNIDADE"), F.col("CO_ESCOLA"), F.col("QT_MATRICULA"))
'''


def test_good_no_hallucination(tmp_path):
    path = write(tmp_path, "good.py", GOOD)
    results = sf.evaluate(path)
    assert all(passed for _i, _t, passed, _e in results), results


def test_bad_flags_hallucination(tmp_path):
    path = write(tmp_path, "bad.py", BAD)
    results = sf.evaluate(path)
    # ambos os itens reprovam porque há campos-fonte MAIÚSCULOS inexistentes
    assert all(not passed for _i, _t, passed, _e in results), results
    ev = " ".join(e for *_x, e in results)
    assert "CO_ESCOLA" in ev and "QT_MATRICULA" in ev


def test_created_columns_not_flagged(tmp_path):
    # colunas criadas (lower e até UPPER via alias) não contam como leitura de fonte
    code = '''
from pyspark.sql import functions as F
def main():
    """cria colunas."""
    df.withColumn("h3_cell", F.lit(1)).select(F.col("h3_cell"))
    df.select(F.col("ID_UNIDADE").alias("CHAVE_NOVA"))
'''
    path = write(tmp_path, "c.py", code)
    results = sf.evaluate(path)
    assert all(passed for _i, _t, passed, _e in results), results
