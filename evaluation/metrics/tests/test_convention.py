import os

import convention as cv
import schema_source as S


def _build_run(tmp_path, declare=True, geoparquet=True, layer="gold"):
    root = tmp_path
    # código gerado
    jobs = root / "jobs" / layer
    jobs.mkdir(parents=True)
    writer = "geoparquet" if geoparquet else "parquet"
    (jobs / "gold_job.py").write_text(
        f'# produz {S.TARGET_TABLE}\n'
        f'df.write.format("{writer}").save("s3a://datalake/{S.APP}/{S.TARGET_TABLE}/gold")\n',
        encoding="utf-8",
    )
    # árvore exportada do bucket
    out = root / "outputs" / "datalake" / S.APP / S.TARGET_TABLE / "gold"
    out.mkdir(parents=True)
    (out / "part-0.parquet").write_text("x", encoding="utf-8")
    # metadata da run
    meta = root / "metadata.toml"
    decl = (
        f'[tables.grid]\nname = "{S.TARGET_TABLE}"\nlevels = ["gold"]\n' if declare else
        '[tables.other]\nname = "outra"\nlevels = ["gold"]\n'
    )
    meta.write_text(f'[data_lake]\ndb="datalake"\napp="{S.APP}"\n\n{decl}', encoding="utf-8")
    return str(root / "jobs"), str(root / "outputs"), str(meta)


def test_all_conventions_pass(tmp_path):
    jobs, outputs, meta = _build_run(tmp_path)
    res = dict((i, ok) for i, ok, _e in cv.evaluate(jobs, outputs, meta, S.APP, S.TARGET_TABLE, S.TARGET_LEVEL))
    esperado = {"2.4.1": True, "2.4.2": True, "2.4.3": True, "2.4.4": True}
    assert {k: res[k] for k in esperado} == esperado, res
    assert "2.4.5" in res  # medallion materializado entra na composição vigente
    assert "2.4.5" in res  # medallion materializado entra na composição vigente


def test_missing_declaration_flags_243(tmp_path):
    jobs, outputs, meta = _build_run(tmp_path, declare=False)
    res = dict((i, ok) for i, ok, _e in cv.evaluate(jobs, outputs, meta, S.APP, S.TARGET_TABLE, S.TARGET_LEVEL))
    assert res["2.4.3"] is False and res["2.4.1"] is True


def test_wrong_layer_flags_241(tmp_path):
    jobs, outputs, meta = _build_run(tmp_path, layer="bronze")
    res = dict((i, ok) for i, ok, _e in cv.evaluate(jobs, outputs, meta, S.APP, S.TARGET_TABLE, S.TARGET_LEVEL))
    assert res["2.4.1"] is False


def test_no_geoparquet_flags_244(tmp_path):
    jobs, outputs, meta = _build_run(tmp_path, geoparquet=False)
    res = dict((i, ok) for i, ok, _e in cv.evaluate(jobs, outputs, meta, S.APP, S.TARGET_TABLE, S.TARGET_LEVEL))
    assert res["2.4.4"] is False
