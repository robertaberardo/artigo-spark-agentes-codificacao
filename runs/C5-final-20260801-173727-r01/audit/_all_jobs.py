# === /d/runs/C5-final-20260801-173727-r01/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

ID_WIDTH = 8

RAW_SCHEMA = T.StructType(
    [
        T.StructField("ID_UNIDADE", T.StringType()),
        T.StructField("NOME_UNIDADE", T.StringType()),
        T.StructField("UF", T.StringType()),
        T.StructField("COD_UF", T.StringType()),
        T.StructField("NOME_MUNICIPIO", T.StringType()),
        T.StructField("COD_MUNICIPIO", T.StringType()),
        T.StructField("SETOR", T.StringType()),
        T.StructField("AREA", T.StringType()),
        T.StructField("SITUACAO", T.StringType()),
        T.StructField("LATITUDE", T.StringType()),
        T.StructField("LONGITUDE", T.StringType()),
        T.StructField("QT_SALAS", T.StringType()),
        T.StructField("TEM_AGUA", T.StringType()),
        T.StructField("TEM_ENERGIA", T.StringType()),
        T.StructField("TEM_ESGOTO", T.StringType()),
        T.StructField("TEM_BANHEIRO", T.StringType()),
        T.StructField("TEM_BIBLIOTECA", T.StringType()),
        T.StructField("TEM_LAB_INFO", T.StringType()),
        T.StructField("TEM_QUADRA", T.StringType()),
        T.StructField("TEM_INTERNET", T.StringType()),
    ]
)

# indicadores de infraestrutura (0/1) que só precisam de dígitos + cast int
FLAG_COLUMNS = [
    "TEM_AGUA",
    "TEM_ENERGIA",
    "TEM_ESGOTO",
    "TEM_BANHEIRO",
    "TEM_BIBLIOTECA",
    "TEM_LAB_INFO",
    "TEM_QUADRA",
    "TEM_INTERNET",
]


def to_int(name: str) -> F.Column:
    """Converte um código numérico curto (texto, sem zeros à esquerda) em ``int``.

    Faz ``trim`` antes do cast para tolerar espaços; texto vazio ou inválido vira
    ``NULL`` (ausência nunca é zero).
    """
    return F.trim(F.col(name)).cast("int").alias(name.lower())


def main():
    """Lê o raw das escolas, padroniza nomes e tipa as colunas para a bronze."""
    spark = SparkSession.builder.appName("educacao-escola-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    id_unidade = ct.zero_pad_code(F.col("ID_UNIDADE"), ID_WIDTH)

    bronze = raw.select(
        id_unidade.alias("id_unidade"),
        ct.normalize_text(F.col("NOME_UNIDADE")).alias("nome_unidade"),
        ct.standardize_uf(F.col("UF")).alias("uf"),
        ct.digits_only(F.col("COD_UF")).alias("cod_uf"),
        ct.normalize_text(F.col("NOME_MUNICIPIO")).alias("nome_municipio"),
        ct.digits_only(F.col("COD_MUNICIPIO")).alias("cod_municipio"),
        to_int("SETOR"),
        to_int("AREA"),
        to_int("SITUACAO"),
        ct.to_coordinate(F.col("LATITUDE")).alias("latitude"),
        ct.to_coordinate(F.col("LONGITUDE")).alias("longitude"),
        to_int("QT_SALAS"),
        *[to_int(c) for c in FLAG_COLUMNS],
    ).filter(id_unidade.isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-173727-r01/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

ID_WIDTH = 8

RAW_SCHEMA = T.StructType(
    [
        T.StructField("ID_UNIDADE", T.StringType()),
        T.StructField("MAT_BASICA", T.StringType()),
        T.StructField("MAT_INFANTIL", T.StringType()),
        T.StructField("MAT_FUNDAMENTAL", T.StringType()),
        T.StructField("MAT_MEDIO", T.StringType()),
        T.StructField("MAT_PROFISSIONAL", T.StringType()),
        T.StructField("MAT_EJA", T.StringType()),
        T.StructField("MAT_ESPECIAL", T.StringType()),
    ]
)

# contagens de matrícula por etapa de ensino
COUNT_COLUMNS = [
    "MAT_BASICA",
    "MAT_INFANTIL",
    "MAT_FUNDAMENTAL",
    "MAT_MEDIO",
    "MAT_PROFISSIONAL",
    "MAT_EJA",
    "MAT_ESPECIAL",
]


def to_count(name: str) -> F.Column:
    """Converte uma contagem de matrículas (texto) em ``int``, ausência como ``NULL``."""
    return F.trim(F.col(name)).cast("int").alias(name.lower())


def main():
    """Lê o raw das matrículas, padroniza nomes e tipa as contagens para a bronze."""
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    id_unidade = ct.zero_pad_code(F.col("ID_UNIDADE"), ID_WIDTH)

    bronze = raw.select(
        id_unidade.alias("id_unidade"),
        *[to_count(c) for c in COUNT_COLUMNS],
    ).filter(id_unidade.isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-173727-r01/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
KRING_K = 1
METERS_PER_KM = 1_000.0
DIST_DECIMALS = 3
ENROLLMENT_DECIMALS = 2

# ponto (lat/long WGS84) usado para o H3 e para a distância geodésica; as
# medidas de distância são feitas sobre estas coordenadas originais, nunca
# sobre a projeção 3857 (só a geometria de saída é reprojetada).
POINT_GEOM = "point"


def prepare_schools(df: DataFrame) -> DataFrame:
    """Adiciona o ponto WGS84 e a célula H3 (resolução 5) de cada escola."""
    with_point = dt.add_point_geometry(df, "latitude", "longitude", POINT_GEOM)
    return dt.attach_h3_index(with_point, POINT_GEOM, H3_LEVEL, "h3_cell").select(
        F.col("id_unidade"),
        F.col("enrollment"),
        F.col("h3_cell"),
        F.col(POINT_GEOM),
    )


def neighbor_membership(schools: DataFrame) -> DataFrame:
    """Mapeia cada escola às células cuja vizinhança (kRing 1) a contém.

    ``ST_H3KRing(cell, 1, False)`` devolve a célula e as 6 adjacentes. Como o
    kRing de distância 1 é simétrico, explodi-lo dá, para cada célula-alvo, todas
    as escolas situadas nela ou numa célula imediatamente adjacente.
    """
    ring = stf.ST_H3KRing(F.col("h3_cell"), F.lit(KRING_K), F.lit(False))
    return schools.select(
        F.col("id_unidade").alias("nb_id"),
        F.col(POINT_GEOM).alias("nb_point"),
        F.explode(ring).alias("target_cell"),
    )


def neighborhood_count(membership: DataFrame) -> DataFrame:
    """Conta as escolas vizinhas por célula (tamanho da vizinhança menos a própria).

    O valor é o mesmo para toda a célula: quantas outras federais em atividade
    estão na célula ou nas adjacentes.
    """
    return membership.groupBy(F.col("target_cell")).agg(
        (F.count(F.lit(1)) - F.lit(1)).cast("long").alias("n_federal_neighbors")
    ).select(
        F.col("target_cell").alias("h3_cell"),
        F.col("n_federal_neighbors"),
    )


def distance_km(geom_a: str, geom_b: str) -> F.Column:
    """Distância geodésica (km) entre dois pontos WGS84 (``ST_DistanceSpheroid``)."""
    return stf.ST_DistanceSpheroid(F.col(geom_a), F.col(geom_b)) / F.lit(METERS_PER_KM)


def neighbor_distance_per_school(schools: DataFrame, membership: DataFrame) -> DataFrame:
    """Distância média de cada escola às suas vizinhas (mesma célula ou adjacentes)."""
    base = schools.select(
        F.col("id_unidade").alias("s_id"),
        F.col("h3_cell").alias("s_cell"),
        F.col(POINT_GEOM).alias("s_point"),
    )
    pairs = base.join(
        membership, base["s_cell"] == membership["target_cell"], how="inner"
    ).filter(F.col("s_id") != F.col("nb_id"))
    return pairs.groupBy(F.col("s_id")).agg(
        F.avg(distance_km("s_point", "nb_point")).alias("avg_dist_neighbors_km")
    ).select(
        F.col("s_id").alias("id_unidade"),
        F.col("avg_dist_neighbors_km"),
    )


def nearest_federal_per_school(schools: DataFrame) -> DataFrame:
    """Distância de cada escola à federal em atividade mais próxima (sem restrição de célula)."""
    left = schools.select(
        F.col("id_unidade").alias("a_id"), F.col(POINT_GEOM).alias("a_point")
    )
    right = schools.select(
        F.col("id_unidade").alias("b_id"), F.col(POINT_GEOM).alias("b_point")
    )
    pairs = left.crossJoin(F.broadcast(right)).filter(F.col("a_id") != F.col("b_id"))
    return pairs.groupBy(F.col("a_id")).agg(
        F.min(distance_km("a_point", "b_point")).alias("dist_nearest_km")
    ).select(
        F.col("a_id").alias("id_unidade"),
        F.col("dist_nearest_km"),
    )


def aggregate_cells(enriched: DataFrame, neighbors: DataFrame) -> DataFrame:
    """Agrega as métricas por célula H3 e anexa a contagem de vizinhos."""
    cells = enriched.groupBy(F.col("h3_cell")).agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum(F.col("enrollment")).cast("long").alias("total_enrollment"),
        F.round(F.avg(F.col("enrollment")), ENROLLMENT_DECIMALS).alias("avg_enrollment"),
        F.round(F.avg(F.col("avg_dist_neighbors_km")), DIST_DECIMALS).alias(
            "avg_dist_neighbors_km"
        ),
        F.round(F.avg(F.col("dist_nearest_km")), DIST_DECIMALS).alias(
            "avg_dist_nearest_km"
        ),
    )
    return dt.join_lookup(cells, neighbors, on="h3_cell", how="left", broadcast=True)


def add_cell_geometry(df: DataFrame) -> DataFrame:
    """Reconstrói o polígono da célula H3 (WGS84) e o reprojeta para EPSG:3857."""
    cell_geom = F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
    return dt.to_web_mercator(df.withColumn("geometry", cell_geom))


def main():
    """Grade H3 (res. 5) das escolas federais em atividade: densidade, matrícula e vizinhança."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = prepare_schools(spark.read.parquet(ORIGEM)).cache()

    membership = neighbor_membership(schools)
    per_school_neighbors = neighbor_distance_per_school(schools, membership)
    per_school_nearest = nearest_federal_per_school(schools)
    neighbors = neighborhood_count(membership)

    enriched = (
        schools.select(F.col("id_unidade"), F.col("h3_cell"), F.col("enrollment"))
        .join(per_school_neighbors, on="id_unidade", how="left")
        .join(per_school_nearest, on="id_unidade", how="left")
    )

    grid = aggregate_cells(enriched, neighbors).transform(add_cell_geometry).select(
        F.col("h3_cell"),
        F.col("n_schools"),
        F.col("total_enrollment"),
        F.col("avg_enrollment"),
        F.col("n_federal_neighbors"),
        F.col("avg_dist_neighbors_km"),
        F.col("avg_dist_nearest_km"),
        F.col("geometry"),
    )

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    grid.orderBy(F.col("n_schools").desc()).show(10, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-173727-r01/workdir/apps/educacao/jobs/silver/educacao_escola_matricula.py ===
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula", "silver")

# setor administrativo e situação de funcionamento relevantes (metadata.toml)
SETOR_FEDERAL = 1
SITUACAO_ATIVA = 1


def keep_federal_active(df: DataFrame) -> DataFrame:
    """Mantém apenas escolas do setor federal e em atividade."""
    is_federal = F.col("setor") == SETOR_FEDERAL
    is_active = F.col("situacao") == SITUACAO_ATIVA
    return df.filter(is_federal & is_active)


def attach_enrollment(escola: DataFrame, matricula: DataFrame) -> DataFrame:
    """Anexa as matrículas da educação básica à escola (join por id_unidade).

    ``MAT_BASICA`` é o total da educação básica; usa-se ``join_no_fanout`` porque a
    matrícula deve ser única por unidade — duplicidade viraria erro, não fan-out.
    """
    enrollment = matricula.select(
        F.col("id_unidade"), F.col("mat_basica").alias("enrollment")
    )
    return dt.join_no_fanout(escola, enrollment, on="id_unidade", how="left")


def main():
    """Escolas federais em atividade, georreferenciadas e com matrículas anexadas."""
    spark = SparkSession.builder.appName("educacao-escola-matricula-silver").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA)

    federal = (
        escola.transform(keep_federal_active)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
    )

    silver = attach_enrollment(federal, matricula).select(
        F.col("id_unidade"),
        F.col("nome_unidade"),
        F.col("uf"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("enrollment"),
    )

    silver.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> silver educacao_escola_matricula: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

