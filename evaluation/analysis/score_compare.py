"""Comparação da tabela final contra a implementação de referência, sob os três predicados.

Reavalia os itens **1.1 a 1.8** de cada execução preservada e emite as **variáveis
registradas** (CRS gravado e casas decimais por coluna), conforme
`docs/ESPECIFICACAO.md §4.2-4.4`.

Emite, para cada coluna `double`, o veredito sob **três predicados numéricos** numa
passada só, de modo que as quatro linhas da escada de sensibilidade saiam do mesmo
pipeline versionado:

* ``registrado``   — igualdade após arredondamento HALF_UP a 2 casas dos dois lados
                     (o predicado que avaliou o lote);
* ``fixo``         — ``|a - g| <= 0,005``;
* ``adaptativo``   — ``|a - g| <= max(0,5 x 10^-d, 1e-6)``, com ``d`` = casas decimais
                     efetivamente gravadas pelo candidato naquela coluna.

**Não depende de Spark.** o ambiente do score não tem Sedona, então a geometria é
comparada em Python: WKB analisado diretamente e, quando os CRS diferem, reprojeção
por fórmula fechada de Web Mercator, que é exata para EPSG:3857. A fidelidade dessa
reimplementação **não é assumida**: ela é provada pela verificação de invariância, que
exige reproduzir os vereditos preservados nos itens não tocados (1.1, 1.2, 1.3, 1.5,
1.8 e o CRS).

`D:/runs` é **somente leitura**: este módulo nunca escreve lá.
"""
from __future__ import annotations

import glob
import json
import math
import os
import struct
from decimal import Decimal, ROUND_HALF_UP

import pyarrow.parquet as pq

TABELA = "educacao_escola_matricula_h3_grid"
CHAVE = "h3_cell"
GEOM = "geometry"
COLUNAS_CONTRATADAS = [
    "h3_cell", "n_schools", "total_enrollment", "avg_enrollment",
    "n_federal_neighbors", "avg_dist_neighbors_km", "avg_dist_nearest_km", "geometry",
]
# item -> coluna comparada (1.1 é o casamento por chave; 1.8 é a geometria)
ITENS_COLUNA = {
    "1.2": "n_schools",
    "1.3": "total_enrollment",
    "1.4": "avg_enrollment",
    "1.5": "n_federal_neighbors",
    "1.6": "avg_dist_neighbors_km",
    "1.7": "avg_dist_nearest_km",
}
PREDICADOS = ("registrado", "fixo", "adaptativo")
RAIO_TERRA = 6378137.0


# --------------------------------------------------------------------------- números

def round_half_up(x: float, casas: int) -> float:
    """Arredondamento HALF_UP, como o do Spark (BigDecimal.setScale), não o do Python.

    O ``round`` embutido usa arredondamento bancário (HALF_EVEN); reproduzir o
    predicado registrado exige HALF_UP, senão a verificação de invariância acusaria
    divergência que é do comparador, não do dado.
    """
    if x is None or math.isnan(x) or math.isinf(x):
        return x
    q = Decimal(1).scaleb(-casas)
    return float(Decimal(repr(x)).quantize(q, rounding=ROUND_HALF_UP))


def detectar_casas(valores) -> int | None:
    """Casas decimais efetivamente gravadas: menor ``d`` tal que round(v, d) == v em todas.

    Feita sobre o **valor gravado**, não sobre a representação em ponto flutuante:
    contar casas de um ``double`` ingenuamente devolveria dezessete.
    Devolve ``None`` quando a coluna é toda nula, e 15 quando nenhum ``d`` fecha,
    caso em que se lê como precisão plena.
    """
    nao_nulos = [v for v in valores if v is not None and not math.isnan(v)]
    if not nao_nulos:
        return None
    for d in range(0, 16):
        if all(round_half_up(v, d) == v for v in nao_nulos):
            return d
    return 15


def tolerancia_adaptativa(d: int | None) -> float:
    if d is None or d >= 15:
        return 1e-6
    return max(0.5 * (10 ** -d), 1e-6)


def compara_valor(a, g, predicado: str, d: int | None) -> bool:
    """Comparação null-safe de um par de valores sob o predicado dado."""
    a_nulo = a is None or (isinstance(a, float) and math.isnan(a))
    g_nulo = g is None or (isinstance(g, float) and math.isnan(g))
    if a_nulo or g_nulo:
        return a_nulo and g_nulo
    if predicado == "registrado":
        return round_half_up(a, 2) == round_half_up(g, 2)
    if predicado == "fixo":
        return abs(a - g) <= 0.005
    return abs(a - g) <= tolerancia_adaptativa(d)


# ------------------------------------------------------------------------- geometria

def _le_wkb_poligono(b: bytes):
    """Analisa um WKB de Polygon ou MultiPolygon e devolve a lista de anéis.

    Formato: ordem de bytes (1) + tipo (4) + conteúdo. Suficiente para a geometria
    desta tarefa, que é sempre o polígono de uma célula da grade.
    """
    if not b:
        return None
    ordem = "<" if b[0] == 1 else ">"
    tipo = struct.unpack_from(ordem + "I", b, 1)[0] & 0xFF
    pos = 5
    aneis = []

    def le_anel(pos):
        n = struct.unpack_from(ordem + "I", b, pos)[0]
        pos += 4
        pontos = struct.unpack_from(ordem + f"{2 * n}d", b, pos)
        pos += 16 * n
        return [(pontos[i], pontos[i + 1]) for i in range(0, 2 * n, 2)], pos

    if tipo == 3:  # Polygon
        n_aneis = struct.unpack_from(ordem + "I", b, pos)[0]
        pos += 4
        for _ in range(n_aneis):
            anel, pos = le_anel(pos)
            aneis.append(anel)
    elif tipo == 6:  # MultiPolygon
        n_pol = struct.unpack_from(ordem + "I", b, pos)[0]
        pos += 4
        for _ in range(n_pol):
            pos += 5  # cabeçalho do polígono interno
            n_aneis = struct.unpack_from(ordem + "I", b, pos)[0]
            pos += 4
            for _ in range(n_aneis):
                anel, pos = le_anel(pos)
                aneis.append(anel)
    else:
        return None
    return aneis


def _para_4326(x: float, y: float) -> tuple[float, float]:
    """Web Mercator inverso, fórmula fechada e exata para EPSG:3857."""
    lon = math.degrees(x / RAIO_TERRA)
    lat = math.degrees(2 * math.atan(math.exp(y / RAIO_TERRA)) - math.pi / 2)
    return lon, lat


def crs_por_magnitude(aneis_por_linha) -> str | None:
    """Infere o CRS de armazenamento pela magnitude da coordenada, como o oráculo faz."""
    maior = 0.0
    achou = False
    for aneis in aneis_por_linha:
        if not aneis:
            continue
        achou = True
        for anel in aneis:
            for x, _y in anel:
                maior = max(maior, abs(x))
    if not achou:
        return None
    return "EPSG:3857" if maior > 180 else "EPSG:4326"


def _normaliza(aneis, crs: str, casas: int = 6):
    """Projeta para 4326 quando necessário e reduz a precisão, como o oráculo fazia."""
    if aneis is None:
        return None
    saida = []
    for anel in aneis:
        pts = []
        for x, y in anel:
            if crs == "EPSG:3857":
                x, y = _para_4326(x, y)
            pts.append((round(x, casas), round(y, casas)))
        saida.append(tuple(pts))
    return tuple(saida)


# ------------------------------------------------------------------------ comparação

def _coluna(tabela, nome):
    return tabela.column(nome).to_pylist() if nome in tabela.schema.names else None


def localizar_gold(run_dir: str) -> str | None:
    padrao = os.path.join(run_dir, "outputs", "**", "*.parquet")
    achados = [p for p in glob.glob(padrao, recursive=True)
               if TABELA in p.replace("\\", "/") and "/gold/" in p.replace("\\", "/")]
    return sorted(achados)[0] if achados else None


def comparar(run_dir: str, ref_path: str) -> dict:
    """Compara a saída de uma execução com a referência. Não escreve nada."""
    r = {"run": os.path.basename(run_dir.rstrip("/\\")), "erro": None}
    gold = localizar_gold(run_dir)
    if not gold:
        r["erro"] = "tabela final ausente"
        return r

    cand = pq.read_table(gold)
    ref = pq.read_table(ref_path)
    cset, rset = set(cand.schema.names), set(ref.schema.names)

    faltando = [c for c in COLUNAS_CONTRATADAS if c not in cset]
    r["gate_schema"] = not faltando
    r["colunas_faltando"] = faltando
    r["colunas_extras"] = sorted(cset - set(COLUNAS_CONTRATADAS) - {GEOM + "_bbox"})
    if faltando:
        r["erro"] = f"colunas contratadas ausentes: {faltando}"
        return r

    # --- 1.1: casamento por chave -------------------------------------------------
    ck, rk = _coluna(cand, CHAVE), _coluna(ref, CHAVE)
    idx_c = {k: i for i, k in enumerate(ck)}
    idx_r = {k: i for i, k in enumerate(rk)}
    comuns = sorted(set(idx_c) & set(idx_r))
    r["n_cand"], r["n_ref"] = len(ck), len(rk)
    r["so_candidato"] = len(set(idx_c) - set(idx_r))
    r["so_referencia"] = len(set(idx_r) - set(idx_c))
    r["itens"] = {}
    r["itens"]["1.1"] = {p: (r["n_cand"] == r["n_ref"] and r["so_candidato"] == 0
                             and r["so_referencia"] == 0) for p in PREDICADOS}

    # --- 1.2 a 1.7: colunas de valor ----------------------------------------------
    r["casas"] = {}
    r["divergencias"] = {}
    for item, col in ITENS_COLUNA.items():
        a_all, g_all = _coluna(cand, col), _coluna(ref, col)
        tipo = str(cand.schema.field(col).type)
        a = [a_all[idx_c[k]] for k in comuns]
        g = [g_all[idx_r[k]] for k in comuns]
        if tipo in ("double", "float"):
            d = detectar_casas(a)
            r["casas"][col] = d
            por_pred, divs = {}, {}
            for p in PREDICADOS:
                ruins = sum(1 for x, y in zip(a, g) if not compara_valor(x, y, p, d))
                por_pred[p] = ruins == 0
                divs[p] = ruins
            r["itens"][item] = por_pred
            r["divergencias"][col] = divs
        else:
            iguais = all((x is None and y is None) or x == y for x, y in zip(a, g))
            r["itens"][item] = {p: iguais for p in PREDICADOS}
            r["divergencias"][col] = {p: sum(1 for x, y in zip(a, g) if x != y)
                                      for p in PREDICADOS}

    # --- 1.8 e CRS ------------------------------------------------------------------
    gc_all, gr_all = _coluna(cand, GEOM), _coluna(ref, GEOM)
    aneis_c_all = [_le_wkb_poligono(b) for b in gc_all]
    aneis_r_all = [_le_wkb_poligono(b) for b in gr_all]
    crs_c = crs_por_magnitude(aneis_c_all)
    crs_r = crs_por_magnitude(aneis_r_all)
    r["crs_candidato"], r["crs_referencia"] = crs_c, crs_r
    r["crs_ok"] = (crs_c == crs_r)

    ruins_geom = 0
    for k in comuns:
        na = _normaliza(aneis_c_all[idx_c[k]], crs_c)
        ng = _normaliza(aneis_r_all[idx_r[k]], crs_r)
        if na != ng:
            ruins_geom += 1
    r["divergencias"][GEOM] = ruins_geom
    r["itens"]["1.8"] = {p: (ruins_geom == 0) for p in PREDICADOS}

    # --- composições ------------------------------------------------------------
    r["composicoes"] = {}
    for p in PREDICADOS:
        oito = all(r["itens"][i][p] for i in
                   ["1.1", "1.2", "1.3", "1.4", "1.5", "1.6", "1.7", "1.8"])
        r["composicoes"][p] = {
            "m1_oito": bool(oito and r["gate_schema"]),
            "registrada_nove": bool(oito and r["gate_schema"] and r["crs_ok"]),
        }
    return r


ROTULOS = {
    "1.1": "h3_cell", "1.2": "n_schools", "1.3": "total_enrollment",
    "1.4": "avg_enrollment", "1.5": "n_federal_neighbors",
    "1.6": "avg_dist_neighbors_km", "1.7": "avg_dist_nearest_km", "1.8": "geometry",
}


def emitir_texto(r: dict, predicado: str = "adaptativo") -> str:
    """Formata o resultado no contrato de instrumento que o montador consome.

    Emite os oito itens pontuados, o gate de contrato de esquema e as duas variáveis
    registradas que nascem aqui: CRS gravado e casas decimais por coluna.
    """
    L = [f"# comparação sob predicado '{predicado}' — {r['run']}"]
    if r.get("erro"):
        L.append(f"# ERRO: {r['erro']}")
        for iid, rot in ROTULOS.items():
            L.append(f"{iid} {rot} FAIL  {r['erro']}")
        L.append(f"GATE schema_contrato {'PASS' if r.get('gate_schema') else 'FAIL'} "
                 f"faltando={r.get('colunas_faltando') or '—'}")
        L.append("VAR crs INDETERMINADO  sem tabela final")
        L.append("VAR casas INDETERMINADO  sem tabela final")
        return "\n".join(L) + "\n"

    L.append(f"# linhas: candidato={r['n_cand']} referência={r['n_ref']} "
             f"só_candidato={r['so_candidato']} só_referência={r['so_referencia']}")
    n_pass = 0
    for iid, rot in ROTULOS.items():
        ok = r["itens"][iid][predicado]
        n_pass += ok
        if iid == "1.8":
            ev = f"divergências={r['divergencias'].get(GEOM, 0)} (CRS-agnóstico)"
        elif iid == "1.1":
            ev = f"só_cand={r['so_candidato']} só_ref={r['so_referencia']}"
        else:
            ev = f"divergências={r['divergencias'].get(rot, {}).get(predicado, 0)}"
            d = r["casas"].get(rot)
            if d is not None:
                ev += f" d={d} tol={tolerancia_adaptativa(d):.1e}"
        L.append(f"{iid} {rot} {'PASS' if ok else 'FAIL'}  {ev}")
    L.append(f"RATIO 1 = {n_pass}/8")
    L.append(f"GATE schema_contrato {'PASS' if r['gate_schema'] else 'FAIL'} "
             f"faltando={r['colunas_faltando'] or '—'} extras={r['colunas_extras'] or '—'}")
    if r["colunas_extras"]:
        L.append(f"AVISO schema_extras: colunas não contratadas: {r['colunas_extras']}")
    L.append(f"VAR crs {r['crs_candidato']}  candidato={r['crs_candidato']} "
             f"referência={r['crs_referencia']} conforme={r['crs_ok']}  [registrado 1.9]")
    casas = ";".join(f"{c}={d}" for c, d in sorted(r["casas"].items()))
    L.append(f"VAR casas {casas}  casas decimais gravadas por coluna double")
    return "\n".join(L) + "\n"


def main() -> int:
    import sys
    runs_root = sys.argv[1] if len(sys.argv) > 1 else "D:/runs"
    ref = sorted(glob.glob(os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "gabaritos", TABELA, "*.parquet")))
    if not ref:
        print("!! parquet da referência não encontrado")
        return 2
    runs = sorted(d for d in glob.glob(os.path.join(runs_root, "*")) if os.path.isdir(d))
    saida = [comparar(d, ref[0]) for d in runs]
    print(json.dumps(saida, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
