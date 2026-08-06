"""Comparação COMPLETA candidato × gabarito — diagnóstico campo a campo.

Vai além do MATCH/MISMATCH binário do ``compare_output.py``: diz exatamente ONDE
diverge. Compara schema (nomes de coluna), contagem, casamento por chave e, para
cada coluna comum, quantas linhas batem e exemplos de divergência. A geometria é
comparada por WKT após reprojetar ambos os lados para EPSG:4326 (CRS inferido pela
magnitude das coordenadas), então diferença de CRS de armazenamento NÃO conta como
divergência de conteúdo — ela é reportada à parte.

**Contrato de schema (gate).** As colunas esperadas são as de
``schema_source.TARGET_SCHEMA`` (P15). Coluna contratada **ausente** reprova o
gate (e o item 1.\\* correspondente). Coluna **a mais** é apenas um **AVISO**:
não reprova nada — a decisão é da pesquisadora (2026-07-31). O mesmo vale para a
coluna de covering bbox do geoparquet, que é artefato do writer (item 2.4.4).

Uso (dentro do container Spark/Sedona):
    spark-submit /data/evaluation/oracle/full_compare.py <cand_dir> <gab_dir> \
        --key h3_cell [--geom geometry]

Sai 0 se o conteúdo bate e o gate de schema passa; 1 caso contrário.
"""
from __future__ import annotations

import os
import sys

from pyspark.sql import functions as F
from sedona.spark import SedonaContext

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import schema_source as S  # noqa: E402


# Itens 1.1–1.8 de M1, na ordem da especificação. Fonte dos rótulos do checklist.
# O CRS de armazenamento NÃO é item pontuado: é variável registrada, emitida como VAR
# (ver `docs/ESPECIFICACAO.md §4.4`). A geometria continua verificada em 1.8, com os
# dois EPSG igualmente aceitos, porque a comparação projeta os dois lados antes de
# julgar o conteúdo.
ITEM_LABELS = [
    ("1.1", "h3_cell"),
    ("1.2", "n_schools"),
    ("1.3", "total_enrollment"),
    ("1.4", "avg_enrollment"),
    ("1.5", "n_federal_neighbors"),
    ("1.6", "avg_dist_neighbors_km"),
    ("1.7", "avg_dist_nearest_km"),
    ("1.8", "geometry"),
]


def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


def casas_gravadas(df, coluna: str, maximo: int = 15):
    """Casas decimais efetivamente gravadas: menor `d` com round(v, d) == v em todas.

    Feita sobre o VALOR gravado, não sobre a representação em ponto flutuante: contar
    casas de um double ingenuamente devolveria dezessete.
    """
    col = F.col(coluna)
    base = df.filter(col.isNotNull())
    if base.limit(1).count() == 0:
        return None
    for d in range(0, maximo + 1):
        if base.filter(F.round(col, d) != col).limit(1).count() == 0:
            return d
    return maximo


def warn(name: str, text: str) -> None:
    """Imprime um AVISO no formato lido pelo `build_checklist` (não reprova nada)."""
    print(f"  AVISO {name}: {text}")


def emit_checklist(verdicts: dict, schema_ok: bool, faltando: list, extras: list) -> None:
    """Imprime o bloco 1.1–1.8 + o gate de schema, no formato do build_checklist."""
    print("\n[CHECKLIST-1] Medida 1 — correção por coluna")
    n_pass = 0
    for iid, label in ITEM_LABELS:
        ok = verdicts.get(iid, False) is True
        n_pass += ok
        print(f"{iid} {label} {'PASS' if ok else 'FAIL'}")
    print(f"RATIO 1 = {n_pass}/8")
    gate = "PASS" if schema_ok else "FAIL"
    print(f"GATE schema_contrato {gate} faltando={faltando or '—'} extras={extras or '—'}")


def load(spark, path):
    try:
        return spark.read.format("geoparquet").load(path), True
    except Exception:
        return spark.read.parquet(path), False


def main():
    cand_path, ref_path = sys.argv[1], sys.argv[2]
    key = arg("--key", "h3_cell")
    geom = arg("--geom", "geometry")

    spark = SedonaContext.create(
        SedonaContext.builder().appName("full-compare")
        .config("spark.driver.memory", "4g").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    cand, _ = load(spark, cand_path)
    ref, _ = load(spark, ref_path)

    print("=" * 70)
    print("COMPARAÇÃO COMPLETA")
    print("  candidato:", cand_path)
    print("  gabarito :", ref_path)
    print("=" * 70)

    cset, rset = set(cand.columns), set(ref.columns)
    bbox_col = geom + "_bbox"
    contracted = set(S.target_column_names())
    print("\n[1] SCHEMA (nomes de coluna)")
    print("  contratado     :", S.target_column_names())
    print("  só no candidato:", sorted(cset - rset) or "—")
    print("  só no gabarito :", sorted(rset - cset) or "—")
    print("  em comum       :", sorted(cset & rset))

    # GATE de contrato: só coluna contratada AUSENTE reprova. Coluna a mais é aviso.
    faltando = sorted(contracted - cset)
    extras = sorted(cset - contracted - {bbox_col})
    schema_ok = not faltando
    if faltando:
        print(f"  !! COLUNAS CONTRATADAS AUSENTES: {faltando}")
    if extras:
        warn("schema_extras", f"colunas não contratadas na saída: {extras}")
        print("     (não reprova — o contrato exige as 8 colunas, não proíbe outras)")

    # divergência de FORMATO geoparquet: covering bbox presente só num lado. É
    # versão do writer (item 2.4.4), não conteúdo — vira AVISO, não reprovação.
    format_ok = None
    if geom in (cset | rset):
        format_ok = (bbox_col in cset) == (bbox_col in rset)
        if not format_ok:
            who = "gabarito" if bbox_col in rset else "candidato"
            warn("writer_geoparquet", f"covering bbox '{bbox_col}' só no {who}")
            print("     (versão diferente do writer geoparquet — ver item 2.4.4)")

    print("\n[2] CONTAGEM")
    nc, nr = cand.count(), ref.count()
    print(f"  candidato={nc}  gabarito={nr}  {'OK' if nc == nr else 'DIFERE'}")

    if key not in cset or key not in rset:
        # falha do AGENTE, não run inválida: emite os 9 itens como FAIL avaliado,
        # senão o gate de completude classificaria a run como inválida (D7).
        print(f"\n!! chave '{key}' ausente em algum lado — sem join possível.")
        emit_checklist({}, schema_ok, faltando, extras)
        spark.stop()
        return 1

    data_cols = sorted((cset & rset) - {key, geom})
    # marcador de PRESENÇA por lado (não usar coluna de dado anulável como proxy:
    # colunas como avg_dist_neighbors_km são NULL em células isoladas e fariam a
    # linha presente parecer ausente — bug que inflava só_candidato/só_gabarito).
    a = (cand.select([F.col(key).cast("string").alias("_k")]
                     + [F.col(c).alias("a_" + c) for c in data_cols])
         .withColumn("_in_a", F.lit(True)))
    g = (ref.select([F.col(key).cast("string").alias("_k")]
                    + [F.col(c).alias("g_" + c) for c in data_cols])
         .withColumn("_in_g", F.lit(True)))
    j = a.join(g, "_k", "full")
    matched = j.filter(F.col("_in_a").isNotNull() & F.col("_in_g").isNotNull())
    m = matched.count()
    print("\n[3] CASAMENTO POR CHAVE (%s)" % key)
    oc = j.filter(F.col("_in_a").isNotNull() & F.col("_in_g").isNull()).count()
    og = j.filter(F.col("_in_a").isNull() & F.col("_in_g").isNotNull()).count()
    print(f"  casados={m}  só_candidato={oc}  só_gabarito={og}")

    print("\n[4] COLUNAS DE DADOS (linhas casadas; tolerância adaptativa por coluna)")
    all_eq = True
    col_ok: dict[str, bool] = {}
    casas_por_coluna: dict[str, int | None] = {}
    for c in data_cols:
        ac, gc = F.col("a_" + c), F.col("g_" + c)
        t = dict(cand.dtypes).get(c, "")
        if t in ("double", "float"):
            # NULL-SAFE (conserto do nulo, §5): linha bate SÓ se ambos NULL, ou
            # ambos não-NULL e iguais dentro da tolerância adaptativa. Assim "0 no lugar de NULL"
            # (ou NULL no lugar de 0) CONTA como divergência — antes o NULL na
            # subtração virava NULL>0=NULL e o filtro descartava (falso MATCH).
            both_null = ac.isNull() & gc.isNull()
            # Régua ADAPTATIVA: a tolerância deriva das casas decimais que o candidato
            # efetivamente gravou, e é monotônica na precisão — candidato mais preciso
            # nunca reprova onde um menos preciso passa. O arredondamento duplo
            # anterior não tinha essa propriedade.
            d = casas_gravadas(matched, "a_" + c)
            tol = max(0.5 * (10 ** -d), 1e-6) if d is not None and d < 15 else 1e-6
            casas_por_coluna[c] = d
            both_eq = ac.isNotNull() & gc.isNotNull() & (F.abs(ac - gc) <= F.lit(tol))
            neq = matched.filter(~(both_null | both_eq)).count()
        else:
            neq = matched.filter(~(ac.eqNullSafe(gc))).count()
        flag = "OK" if neq == 0 else f"DIVERGE ({neq})"
        col_ok[c] = neq == 0
        print(f"  {c:<22} {flag}")
        if neq:
            all_eq = False
            matched.filter(~(both_null | both_eq)).select("_k", ac, gc).show(5, truncate=False)

    print("\n[5] GEOMETRIA (%s)" % geom)
    geom_content_ok = None  # mesmas células (ignorando CRS)
    crs_ok = None           # mesmo CRS de armazenamento (parte do contrato)
    if geom in (cset & rset):
        def with_wkt(df, col):
            # infere CRS pela magnitude: |coord|>180 => métrico (3857); senão graus (4326)
            mx = df.select(F.max(F.abs(F.expr(f"ST_X(ST_Centroid({col}))")))).collect()[0][0] or 0
            src = "EPSG:3857" if mx > 180 else "EPSG:4326"
            w = df.withColumn(
                "_wkt",
                F.expr(f'ST_AsText(ST_ReducePrecision(ST_Transform({col}, "{src}", "EPSG:4326"), 6))')
                if src != "EPSG:4326"
                else F.expr(f"ST_AsText(ST_ReducePrecision({col}, 6))"),
            )
            return w, src
        cw, csrc = with_wkt(cand.select(F.col(key).cast("string").alias("_k"), geom), geom)
        gw, gsrc = with_wkt(ref.select(F.col(key).cast("string").alias("_k"), geom), geom)
        crs_ok = csrc == gsrc
        print(f"  CRS de armazenamento : candidato={csrc}  gabarito={gsrc}  -> {'OK' if crs_ok else 'DIVERGE'}")
        jg = (cw.select("_k", F.col("_wkt").alias("a_wkt"))
              .join(gw.select("_k", F.col("_wkt").alias("g_wkt")), "_k", "inner"))
        gneq = jg.filter(F.col("a_wkt") != F.col("g_wkt")).count()
        geom_content_ok = gneq == 0
        print(f"  conteúdo (mesmas células, reprojetado p/ 4326): {'OK' if geom_content_ok else f'DIVERGE ({gneq})'}")
        geom_stored_ok = geom_content_ok and crs_ok
        print(f"  geometria ARMAZENADA (conteúdo + CRS): {'OK' if geom_stored_ok else 'DIVERGE'}")
        if not crs_ok:
            print(f"  !! CRS INCORRETO: o gabarito grava em {gsrc}; o candidato gravou em {csrc}.")
            print(f"     É DIVERGÊNCIA de contrato (não é só cosmético) — a geometria persistida difere.")

    # --- CHECKLIST Medida 1 (itens 1.1–1.9, formato X.Y ... PASS/FAIL) ---------
    # Emitido no mesmo padrão das métricas 2.* para o build_checklist.py montar o
    # checklist.tsv 1:1 (nada de parsing frágil de prosa).
    key_match = (nc == nr) and (oc == 0) and (og == 0)
    verdicts = {
        "1.1": key_match,
        "1.8": geom_content_ok is True,
    }
    for iid, label in ITEM_LABELS[1:7]:  # 1.2–1.7: uma coluna de dados cada
        verdicts[iid] = col_ok.get(label, False) is True
    emit_checklist(verdicts, schema_ok, faltando, extras)
    # Variáveis registradas: computadas e reportadas, sem pontuar (§4.4).
    print(f"VAR crs {'EPSG:3857' if crs_ok else 'DIVERGE'}  candidato={csrc if geom in (cset & rset) else '—'} "
          f"conforme={crs_ok}  [registrado 1.9]")
    print("VAR casas " + (";".join(f"{k}={v}" for k, v in sorted(casas_por_coluna.items())) or "—")
          + "  casas decimais gravadas por coluna double")

    print("\n" + "=" * 70)
    geom_stored_ok = (
        None if geom_content_ok is None else (bool(geom_content_ok) and bool(crs_ok))
    )
    valores_iguais = nc == nr and oc == 0 and og == 0 and all_eq  # ignora schema/geom
    # ≡ (9/9 itens ∧ gate de schema): o veredito do oráculo e o `checklist.tsv`
    # não podem discordar. Colunas extras e versão do writer são AVISO.
    # M1 é precisão-agnóstica e CRS-agnóstica: o veredito usa o CONTEÚDO da geometria,
    # não a projeção de armazenamento, que é variável registrada.
    completamente_igual = (
        schema_ok and valores_iguais and (geom_content_ok in (None, True))
    )
    tri = lambda b: "SIM" if b else ("N/A" if b is None else "NÃO")
    print("VEREDITO")
    print(f"  colunas contratadas presentes: {tri(schema_ok)}")
    print(f"  avisos                       : "
          f"extras={extras or '—'} writer_bbox_igual={tri(format_ok)}")
    print(f"  valores idênticos            : {tri(valores_iguais)}")
    print(f"  geometria: mesmas células    : {tri(geom_content_ok)}")
    print(f"  geometria: CRS correto       : {tri(crs_ok)}")
    print(f"  geometria ARMAZENADA         : {tri(geom_stored_ok)}")
    print(f"  COMPLETAMENTE IGUAL          : {tri(completamente_igual)}")
    print("=" * 70)
    spark.stop()
    return 0 if completamente_igual else 1


if __name__ == "__main__":
    sys.exit(main())
