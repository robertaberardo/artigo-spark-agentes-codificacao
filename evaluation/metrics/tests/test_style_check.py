from conftest import write

import style_check as sc

CLEAN = '''
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    """job limpo."""
    a = spark.read.schema(SCH).option("sep", ";").csv(p)
    b = spark.read.schema(SCH2).csv(q)
    j = a.join(b, on="ID_UNIDADE", how="inner")
    out = j.select(F.col("ID_UNIDADE"), F.count(F.lit(1)).alias("n"))
    out.write.mode("overwrite").format("geoparquet").save(dest)
'''

DIRTY = '''
from pyspark.sql import functions as F

def main():
    """job com anti-padroes."""
    a = spark.read.option("inferSchema", True).csv(p)
    b = a.withColumnRenamed("x", "y").withColumn("z", F.expr("ST_X(g)"))
    c = b.join(other)
    return c
'''


def _by_id(rules):
    return {rid: (passed, kind) for rid, kind, passed, _ev in rules}


def test_clean_passes_strict_rules(tmp_path):
    path = write(tmp_path, "clean.py", CLEAN)
    r = _by_id(sc.check_file(path))
    for rid in ["2.3.1", "2.3.2", "2.3.3", "2.3.4", "2.3.5", "2.3.6", "2.3.7", "2.3.8", "2.3.9"]:
        assert r[rid][0], f"{rid} deveria passar no código limpo"


def test_dirty_flags_targets(tmp_path):
    path = write(tmp_path, "dirty.py", DIRTY)
    r = _by_id(sc.check_file(path))
    assert not r["2.3.4"][0], "withColumnRenamed deveria reprovar 2.3.4"
    assert not r["2.3.5"][0], "inferSchema deveria reprovar 2.3.5"
    assert not r["2.3.6"][0], "F.expr deveria reprovar 2.3.6"
    assert not r["2.3.8"][0], "join sem how deveria reprovar 2.3.8"


def test_sixteen_rules(tmp_path):
    path = write(tmp_path, "clean.py", CLEAN)
    assert len(sc.check_file(path)) == 16
