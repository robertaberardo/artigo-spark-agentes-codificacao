import os

import build_checklist as bc


def _audit(tmp_path, oracle_pass=True, style_fail_one=False, gate="PASS", extras=None):
    a = tmp_path / "audit"
    a.mkdir()
    # oráculo 1.1-1.8 + gate de contrato de schema + variáveis registradas
    lines = [f"1.{k} col{k} {'PASS' if oracle_pass else 'FAIL'}" for k in range(1, 9)]
    # variáveis registradas: não pontuam, mas o gate de completude as exige
    lines.append("VAR crs EPSG:3857  conforme=True  [registrado 1.9]")
    lines.append("VAR casas avg_enrollment=3  casas gravadas")
    if extras:
        lines.append(f"AVISO schema_extras: colunas não contratadas na saída: {extras}")
    if gate:
        lines.append(f"GATE schema_contrato {gate} faltando=— extras=—")
    (a / "oracle_educacao_escola_matricula_h3_grid.txt").write_text("\n".join(lines), encoding="utf-8")
    # schema 2.1.1-2.1.2
    (a / "schema_fields.txt").write_text("2.1.1 escola PASS  x\n2.1.2 matricula PASS  y\n", encoding="utf-8")
    # reuso 2.2.1-2.2.10 (classes) + a variável registrada de deduplicação
    (a / "reuse_check.txt").write_text(
        "\n".join(f"2.2.{k} classe PASS" for k in range(1, 11))
        + "\nVAR dedupe nao_usado  ausente  [registrado 2.2.6]\n", encoding="utf-8")
    # style 2.3.1-2.3.16
    style = [f"2.3.{k} [strict] {'FAIL' if (style_fail_one and k == 4) else 'PASS'}  ev" for k in range(1, 17)]
    (a / "style_check.txt").write_text("\n".join(style), encoding="utf-8")
    # convention 2.4.1-2.4.5
    (a / "convention.txt").write_text(
        "\n".join(f"2.4.{k} PASS  ev" for k in range(1, 6)), encoding="utf-8")
    return str(a)


def test_complete_and_correct(tmp_path):
    audit = _audit(tmp_path)
    out = str(tmp_path / "checklist.tsv")
    rc = bc.main.__wrapped__ if hasattr(bc.main, "__wrapped__") else None
    # chamar via argv
    import sys
    sys.argv = ["build_checklist.py", "--audit-dir", audit, "--out", out]
    code = bc.main()
    assert code == 0
    rows = open(out, encoding="utf-8").read().splitlines()
    assert rows[0].split("	") == ["item", "tipo", "resultado", "registrado",
                                   "fonte", "evidencia"]
    assert len(rows) == 1 + 41 + 3  # cabeçalho + 41 pontuados + 3 variáveis
    assert all("NAO_AVALIADO" not in r for r in rows[1:])


def test_completeness_gate_fails_on_missing(tmp_path):
    audit = _audit(tmp_path)
    os.remove(os.path.join(audit, "convention.txt"))  # remove 2.4.*
    out = str(tmp_path / "checklist.tsv")
    import sys
    sys.argv = ["build_checklist.py", "--audit-dir", audit, "--out", out]
    code = bc.main()
    assert code == 1  # gate de completude reprova
    content = open(out, encoding="utf-8").read()
    assert "2.4.1\tpontuado\tNAO_AVALIADO" in content


def test_first_occurrence_wins_and_ids(tmp_path):
    assert len(bc.expected_items()) == 41
    assert bc.expected_items()[0] == "1.1"
    assert "2.3.16" in bc.expected_items()


def _run(tmp_path, **kw):
    """Roda o montador e devolve (exit_code, stdout, conteúdo do checklist.tsv)."""
    import contextlib
    import io
    import sys

    audit = _audit(tmp_path, **kw)
    out = str(tmp_path / "checklist.tsv")
    sys.argv = ["build_checklist.py", "--audit-dir", audit, "--out", out]
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        code = bc.main()
    return code, buf.getvalue(), open(out, encoding="utf-8").read()


def test_gate_de_schema_reprova_medida1_sem_invalidar_a_run(tmp_path):
    # coluna contratada ausente: gate FAIL -> Medida 1 INCORRETA, mas a run segue
    # válida (os 41 itens e as 3 variáveis foram avaliados) — falha do agente, não do harness.
    code, stdout, content = _run(tmp_path, gate="FAIL")
    assert code == 0
    assert "M1 (correção binária): INCORRETA" in stdout
    assert "gate schema_contrato: FAIL" in stdout
    assert len(content.splitlines()) == 1 + 41 + 3  # o gate não vira item


def test_colunas_extras_apenas_avisam(tmp_path):
    # decisão 2026-07-31: coluna a mais é PASS + aviso, nunca reprovação
    code, stdout, _ = _run(tmp_path, extras="['lixo']")
    assert code == 0
    assert "M1 (correção binária): CORRETA" in stdout
    assert "AVISO schema_extras" in stdout


def test_gate_ausente_nao_conta_como_correta(tmp_path):
    # log de oráculo antigo/truncado, sem a linha GATE: não dá para afirmar CORRETA
    code, stdout, _ = _run(tmp_path, gate=None)
    assert code == 0
    assert "M1 (correção binária): INCORRETA" in stdout
    assert "gate schema_contrato: AUSENTE" in stdout
