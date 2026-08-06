# === /d/runs/C5-final-20260801-124146-r01/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# O CSV do Censo Escolar chega em maiúsculas e separado por ';'. O schema é
# aplicado por posição (header descartado), já renomeando para snake_case.
RAW_SCHEMA = StructType(
    [
        StructField("id_unidade", StringType()),
        StructField("nome_unidade", StringType()),
        StructField("uf", StringType()),
        StructField("cod_uf", StringType()),
        StructField("nome_municipio", StringType()),
        StructField("cod_municipio", StringType()),
        StructField("setor", StringType()),
        StructField("area", StringType()),
        StructField("situacao", StringType()),
        StructField("latitude", StringType()),
        StructField("longitude", StringType()),
        StructField("qt_salas", StringType()),
        StructField("tem_agua", StringType()),
        StructField("tem_energia", StringType()),
        StructField("tem_esgoto", StringType()),
        StructField("tem_banheiro", StringType()),
        StructField("tem_biblioteca", StringType()),
        StructField("tem_lab_info", StringType()),
        StructField("tem_quadra", StringType()),
        StructField("tem_internet", StringType()),
    ]
)


def main():
    """Lê o raw das escolas, tipa as colunas e grava a bronze."""
    spark = SparkSession.builder.appName("educacao-escola-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(raw["id_unidade"], 8).alias("id_unidade"),
        ct.normalize_text(raw["nome_unidade"]).alias("nome_unidade"),
        ct.standardize_uf(raw["uf"]).alias("uf"),
        ct.digits_only(raw["cod_municipio"]).alias("cod_municipio"),
        raw["setor"].cast("int").alias("setor"),
        raw["area"].cast("int").alias("area"),
        raw["situacao"].cast("int").alias("situacao"),
        ct.to_coordinate(raw["latitude"]).alias("latitude"),
        ct.to_coordinate(raw["longitude"]).alias("longitude"),
        raw["qt_salas"].cast("int").alias("qt_salas"),
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-124146-r01/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

# Contagens de matrículas por etapa; schema aplicado por posição (header ';').
RAW_SCHEMA = StructType(
    [
        StructField("id_unidade", StringType()),
        StructField("mat_basica", StringType()),
        StructField("mat_infantil", StringType()),
        StructField("mat_fundamental", StringType()),
        StructField("mat_medio", StringType()),
        StructField("mat_profissional", StringType()),
        StructField("mat_eja", StringType()),
        StructField("mat_especial", StringType()),
    ]
)

# Colunas de contagem: ausência é NULL (desconhecido), não zero.
CONTAGENS = [
    "mat_basica",
    "mat_infantil",
    "mat_fundamental",
    "mat_medio",
    "mat_profissional",
    "mat_eja",
    "mat_especial",
]


def main():
    """Lê o raw das matrículas, tipa as contagens e grava a bronze."""
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(raw["id_unidade"], 8).alias("id_unidade"),
        *[raw[c].cast("int").alias(c) for c in CONTAGENS],
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-124146-r01/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import DataFrame, functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5              # resolução da grade H3
SETOR_FEDERAL = 1
SITUACAO_ATIVA = 1
KRING_K = 1              # vizinhança: a própria célula + as adjacentes imediatas
METERS_PER_KM = 1000.0

COLUNAS = [
    "h3_cell",
    "n_schools",
    "total_enrollment",
    "avg_enrollment",
    "n_federal_neighbors",
    "avg_dist_neighbors_km",
    "avg_dist_nearest_km",
    "geometry",
]


def federal_ativa_base(spark) -> DataFrame:
    """Escolas federais em atividade, com ponto WGS84 e célula H3 nível 5.

    H3 e distância geodésica operam sobre a lat/long WGS84 original (não sobre a
    geometria 3857 do silver): reconstrói o ponto a partir de latitude/longitude.
    """
    silver = spark.read.format("geoparquet").load(ORIGEM)
    return (
        silver.filter(
            (F.col("setor") == SETOR_FEDERAL) & (F.col("situacao") == SITUACAO_ATIVA)
        )
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude", "geom_wgs"))
        .transform(lambda d: dt.attach_h3_index(d, "geom_wgs", H3_LEVEL, "h3_cell"))
        .select("id_unidade", "h3_cell", "matriculas", "geom_wgs")
    )


def cell_aggregates(base: DataFrame) -> DataFrame:
    """Escolas, matrículas e polígono H3 (em 3857) por célula."""
    return (
        base.groupBy("h3_cell")
        .agg(
            F.count(F.lit(1)).cast("long").alias("n_schools"),
            F.sum("matriculas").cast("long").alias("total_enrollment"),
            F.avg("matriculas").alias("avg_enrollment"),
        )
        .withColumn(
            "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
        )
        .transform(dt.to_web_mercator)  # polígono H3 (4326) -> 3857, convenção de saída
    )


def cell_kring(base: DataFrame) -> DataFrame:
    """Para cada célula, o anel-1 de células (a própria + as adjacentes)."""
    return base.select("h3_cell").distinct().withColumn(
        "kring", stf.ST_H3KRing(F.col("h3_cell"), KRING_K, False)
    )


def neighbor_counts(base: DataFrame, kring: DataFrame) -> DataFrame:
    """Nº de escolas federais na vizinhança da célula (constante por célula).

    Soma, sobre a própria célula e as adjacentes, quantas escolas cada uma tem.
    """
    per_cell = base.groupBy("h3_cell").agg(F.count(F.lit(1)).alias("cnt"))
    exploded = kring.select("h3_cell", F.explode("kring").alias("neighbor_cell"))
    return (
        exploded.join(
            per_cell.withColumnRenamed("h3_cell", "neighbor_cell"),
            on="neighbor_cell",
            how="left",
        )
        .groupBy("h3_cell")
        .agg(F.sum("cnt").cast("long").alias("n_federal_neighbors"))
    )


def distance_aggregates(base: DataFrame, kring: DataFrame) -> DataFrame:
    """Distâncias geodésicas por escola, agregadas por célula.

    Um self cross-join (conjunto pequeno) dá, para cada escola: a distância à
    federal mais próxima (qualquer célula) e a média às vizinhas (mesma célula ou
    adjacente). A média por célula ignora escolas sem vizinha (NULL).
    """
    base_k = base.join(kring, on="h3_cell", how="inner")
    left = base_k.alias("s")
    right = base.alias("t")

    dist_km = stf.ST_DistanceSpheroid(
        F.col("s.geom_wgs"), F.col("t.geom_wgs")
    ) / F.lit(METERS_PER_KM)
    is_neighbor = F.array_contains(F.col("s.kring"), F.col("t.h3_cell"))

    pairs = left.join(
        right, F.col("s.id_unidade") != F.col("t.id_unidade")
    ).select(
        F.col("s.id_unidade").alias("id_unidade"),
        F.col("s.h3_cell").alias("h3_cell"),
        dist_km.alias("dist_km"),
        F.when(is_neighbor, dist_km).alias("neighbor_dist_km"),
    )

    per_school = pairs.groupBy("id_unidade", "h3_cell").agg(
        F.min("dist_km").alias("nearest_km"),
        F.avg("neighbor_dist_km").alias("neighbor_mean_km"),
    )
    return per_school.groupBy("h3_cell").agg(
        F.avg("neighbor_mean_km").alias("avg_dist_neighbors_km"),
        F.avg("nearest_km").alias("avg_dist_nearest_km"),
    )


def main():
    """Grade H3 nível 5 das escolas federais em atividade."""
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    base = federal_ativa_base(spark).cache()  # reusado em todos os passos
    kring = cell_kring(base)

    grid = (
        cell_aggregates(base)
        .join(neighbor_counts(base, kring), on="h3_cell", how="left")
        .join(distance_aggregates(base, kring), on="h3_cell", how="left")
        .select(*COLUNAS)
    )

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-124146-r01/workdir/apps/educacao/jobs/silver/educacao_escola_matricula.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula", "silver")

COLUNAS = [
    "id_unidade",
    "uf",
    "setor",
    "situacao",
    "latitude",
    "longitude",
    "matriculas",
    "geometry",
]


def main():
    """Consolida escolas + matrículas georreferenciadas (ponto em EPSG:3857).

    A lat/long WGS84 original é preservada nas colunas ``latitude``/``longitude``
    para que a gold meça distâncias geodésicas antes de qualquer projeção.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    # matrícula da educação básica é o total de matrículas da escola
    matricula = spark.read.parquet(MATRICULA).select(
        "id_unidade", F.col("mat_basica").alias("matriculas")
    )

    consolidado = (
        escola.transform(
            lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude")
        )
        .transform(
            lambda d: dt.join_lookup(d, matricula, "id_unidade", ["matriculas"])
        )
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(*COLUNAS)
    )

    consolidado.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_matricula: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

