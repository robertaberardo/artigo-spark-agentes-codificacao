"""GERADOR DO GABARITO — `educacao_escola_matricula_h3_grid` (8 colunas).

Implementação de REFERÊNCIA da tarefa: lê o raw de-brandado do lake
(`s3a://datalake/educacao/educacao_{escola,matricula}/raw`), faz a limpeza
canônica e produz a tabela-alvo. É o "certo" contra o qual o oráculo compara.

Segue o `CODING_STANDARDS.md` e reusa as `utils/` — o mesmo que se cobra do
agente. O contrato (nomes, ordem, resolução H3, CRS, códigos) vem de
`evaluation/schema_source.py` (P15); nada de constante duplicada aqui.

As três colunas de isolamento são resolvidas POR ESCOLA e só então promediadas
por célula — a convenção está detalhada em `docs/ESPECIFICACAO.md §4.2` e nas
docstrings de `ring_stats`/`nearest_stats`/`aggregate_grid`.

Determinismo: `spark.sql.shuffle.partitions` fixo; `verify_determinism.sh`
regenera 2× e exige MATCH (a 6 casas).

Uso (container Spark/Sedona; S3A e PYTHONPATH já configurados):
    spark-submit /data/evaluation/gabaritos/build_gabarito.py [--out <dir>]
"""
from __future__ import annotations

import os
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_functions as stf

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import schema_source as S  # noqa: E402

from utils.column_transforms import (  # noqa: E402
    blank_to_null,
    to_coordinate,
    zero_pad_code,
)
from utils.dataframe_transforms import (  # noqa: E402
    add_point_geometry,
    deduplicate_by_key,
    filter_valid_coordinates,
    h3_join,
    join_no_fanout,
    to_web_mercator,
)

FEDERAL = S.SEMANTICS["SETOR"]["federal"]
ACTIVE = S.SEMANTICS["SITUACAO"]["em_atividade"]
WGS84 = "EPSG:4326"
RING_K = 1
KEY_WIDTH = 8
METERS_PER_KM = 1000.0
SHUFFLE_PARTITIONS = "16"
RAW_OPTIONS = {"header": "true", "sep": ";", "encoding": "ISO-8859-1"}


def arg(flag: str, default: str | None = None) -> str | None:
    """Lê o valor de uma flag de linha de comando (``--x valor``)."""
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


def raw_path(table: str) -> str:
    """Caminho da camada raw de uma tabela-fonte no lake."""
    return f"s3a://{S.DB}/{S.APP}/{S.APP}_{table}/raw"


def geodesic_km(geom_a, geom_b):
    """Distância geodésica (km) entre dois pontos WGS84, sobre o elipsoide.

    Não é a distância planar da projeção: em Web Mercator o metro se estica com
    a latitude, e o número sairia errado nas latitudes do Brasil.
    """
    return stf.ST_DistanceSpheroid(geom_a, geom_b) / F.lit(METERS_PER_KM)


def cell_polygon(cell_col):
    """Polígono da célula H3 (em WGS84) a partir do id da célula."""
    return F.element_at(stf.ST_H3ToGeom(F.array(cell_col)), 1)


def read_raw(spark, table: str) -> DataFrame:
    """Lê o CSV cru de uma tabela-fonte com as opções do Censo.

    O Censo aterrissa em ISO-8859-1, separado por ``;`` e sem tipagem — tudo
    string. O schema é declarado no ``select`` de cada limpeza, não inferido.
    """
    return spark.read.options(**RAW_OPTIONS).csv(raw_path(table))


def clean_schools(df: DataFrame) -> DataFrame:
    """Seleciona as federais em atividade com coordenada utilizável.

    Os filtros comparam os códigos crus (``1`` federal, ``1`` em atividade): o
    Censo não traz colunas booleanas e traduzi-las aqui esconderia a semântica.
    Coordenada ausente (56 das 716 federais ativas de 2025) impede o cálculo da
    célula H3 — a escola sai do universo do estudo.
    """
    is_federal = F.col("setor") == F.lit(FEDERAL)
    is_active = F.col("situacao") == F.lit(ACTIVE)
    schools = df.select(
        zero_pad_code(F.col(S.JOIN_KEY), KEY_WIDTH).alias("co_entidade"),
        blank_to_null(F.col("SETOR")).alias("setor"),
        blank_to_null(F.col("SITUACAO")).alias("situacao"),
        to_coordinate(F.col("LATITUDE")).alias("latitude"),
        to_coordinate(F.col("LONGITUDE")).alias("longitude"),
    ).filter(is_federal & is_active)
    schools = filter_valid_coordinates(schools, "latitude", "longitude")
    return deduplicate_by_key(schools, ["co_entidade"])


def clean_enrollment(df: DataFrame) -> DataFrame:
    """Seleciona a chave e o total de matrículas da educação básica.

    ``MAT_BASICA`` já é o total pré-agregado. Somar os componentes
    (``MAT_INFANTIL/FUNDAMENTAL/MEDIO/PROFISSIONAL``) dá outro número — em 568 das
    660 federais de 2025 — e é o erro que ``total_enrollment`` existe para pegar.
    """
    enrollment = df.select(
        zero_pad_code(F.col(S.JOIN_KEY), KEY_WIDTH).alias("co_entidade"),
        blank_to_null(F.col("MAT_BASICA")).cast("long").alias("qt_mat_bas"),
    )
    return deduplicate_by_key(enrollment, ["co_entidade"])


def build_pool(schools: DataFrame, enrollment: DataFrame) -> DataFrame:
    """Monta o universo do cálculo: escola + matrícula + ponto + célula H3.

    A chave é única dos dois lados (``join_no_fanout`` levanta se não for), então
    o join não multiplica escolas.
    """
    pool = join_no_fanout(schools, enrollment, on="co_entidade", how="inner")
    pool = add_point_geometry(pool, "latitude", "longitude", "geometry")
    pool = h3_join(pool, level=S.H3_RESOLUTION, geom_col="geometry", crs=WGS84)
    return pool.select("co_entidade", "qt_mat_bas", "h3_cell", "geometry")


def ring_stats(pool: DataFrame) -> DataFrame:
    """Vizinhança federal→federal no k-ring k=1, por escola.

    O anel de uma escola são as 7 células (a dela + as 6 adjacentes); vizinhas
    são as OUTRAS federais ativas ali dentro, inclusive as do mesmo hexágono.
    Escola sem vizinha simplesmente não aparece no resultado — quem chama decide
    o que é ausência.
    """
    # lados com nomes disjuntos: o self-join resolve por nome, sem qualificador
    origin = pool.select(
        F.col("co_entidade").alias("origin_id"),
        F.col("geometry").alias("origin_geom"),
        F.explode(stf.ST_H3KRing(F.col("h3_cell"), RING_K, False)).alias("ring_cell"),
    )
    neighbor = pool.select(
        F.col("co_entidade").alias("neighbor_id"),
        F.col("geometry").alias("neighbor_geom"),
        F.col("h3_cell").alias("neighbor_cell"),
    )

    pairs = origin.join(
        neighbor, F.col("ring_cell") == F.col("neighbor_cell"), how="inner"
    ).filter(F.col("origin_id") != F.col("neighbor_id"))
    pairs = pairs.withColumn(
        "dist_km", geodesic_km(F.col("origin_geom"), F.col("neighbor_geom"))
    )

    return pairs.groupBy("origin_id").agg(
        F.count(F.lit(1)).alias("neighbor_count"),
        F.avg("dist_km").alias("neighbor_avg_km"),
    ).select(
        F.col("origin_id").alias("co_entidade"),
        F.col("neighbor_count"),
        F.col("neighbor_avg_km"),
    )


def nearest_stats(pool: DataFrame) -> DataFrame:
    """Distância à federal ativa mais próxima, por escola (mínimo global).

    Sem restrição de célula ou de anel — é o par mais próximo em todo o país. São
    ~660 escolas, então o produto cartesiano (~435 mil pares) é barato e o mínimo
    é único, o que mantém o gabarito determinístico.
    """
    source = pool.select(
        F.col("co_entidade").alias("source_id"),
        F.col("geometry").alias("source_geom"),
    )
    target = pool.select(
        F.col("co_entidade").alias("target_id"),
        F.col("geometry").alias("target_geom"),
    )

    pairs = source.crossJoin(target).filter(F.col("source_id") != F.col("target_id"))
    pairs = pairs.withColumn(
        "dist_km", geodesic_km(F.col("source_geom"), F.col("target_geom"))
    )

    return pairs.groupBy("source_id").agg(
        F.min("dist_km").alias("nearest_km"),
    ).select(
        F.col("source_id").alias("co_entidade"),
        F.col("nearest_km"),
    )


def attach_isolation(pool: DataFrame, ring: DataFrame, nearest: DataFrame) -> DataFrame:
    """Anexa as estatísticas de isolamento à escola, tratando ausência.

    Sem vizinha no anel, a CONTAGEM é ``0`` (contar o vazio dá zero), mas a
    DISTÂNCIA média fica ``NULL``: mediar o vazio não dá 0 km, e dizer 0 seria
    afirmar que a vizinha está em cima da escola.
    """
    fed = join_no_fanout(pool, ring, on="co_entidade", how="left")
    fed = join_no_fanout(fed, nearest, on="co_entidade", how="left")
    return fed.withColumn("neighbor_count", F.coalesce(F.col("neighbor_count"), F.lit(0)))


def aggregate_grid(fed: DataFrame) -> DataFrame:
    """Agrega as escolas por célula H3 e devolve as 8 colunas contratadas.

    As três colunas de isolamento entram como MÉDIA entre as escolas da célula.
    ``neighbor_count`` é constante dentro da célula (todas partilham o mesmo
    anel), então a média devolve o próprio valor; ``avg_dist_neighbors_km`` e
    ``avg_dist_nearest_km`` são de fato médias de valores por escola. As
    agregações do Spark já ignoram ``NULL`` — nada é preenchido com 0 antes.
    """
    grid = fed.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("qt_mat_bas").cast("long").alias("total_enrollment"),
        F.avg("qt_mat_bas").cast("double").alias("avg_enrollment"),
        F.avg("neighbor_count").cast("long").alias("n_federal_neighbors"),
        F.avg("neighbor_avg_km").cast("double").alias("avg_dist_neighbors_km"),
        F.avg("nearest_km").cast("double").alias("avg_dist_nearest_km"),
    )
    # a célula sai do H3 em WGS84; o silver deste lake persiste geometria em 3857
    grid = grid.withColumn("geometry", cell_polygon(F.col("h3_cell")))
    grid = to_web_mercator(grid, "geometry")
    return grid.select(*S.target_column_names())


def report(grid: DataFrame) -> None:
    """Imprime o resumo do gabarito (total de células e proporção de isoladas)."""
    n_cells = grid.count()
    n_isolated = grid.filter(F.col("avg_dist_neighbors_km").isNull()).count()
    share = 100 * n_isolated / max(n_cells, 1)
    print("=" * 60)
    print(f"gabarito: {n_cells} células | isoladas (NULL)={n_isolated} ({share:.1f}%)")
    print("=" * 60)
    grid.orderBy(F.col("n_schools").desc()).show(8, truncate=False)


def main() -> None:
    """Gera o gabarito da tabela-alvo e persiste em geoparquet."""
    level = int(arg("--h3", str(S.H3_RESOLUTION)))
    out = arg("--out", f"/data/gabaritos/{S.TARGET_TABLE}")
    if level != S.H3_RESOLUTION:
        print(f"!! resolução {level} difere do contrato ({S.H3_RESOLUTION})")

    spark = SedonaContext.create(
        SedonaContext.builder().appName("build-gabarito")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = clean_schools(read_raw(spark, "escola"))
    enrollment = clean_enrollment(read_raw(spark, "matricula"))
    pool = build_pool(schools, enrollment).cache()

    fed = attach_isolation(pool, ring_stats(pool), nearest_stats(pool))
    grid = aggregate_grid(fed)

    report(grid)
    grid.coalesce(1).write.mode("overwrite").format("geoparquet").save(out)
    print(f">> gabarito salvo em {out}")
    spark.stop()


if __name__ == "__main__":
    main()
