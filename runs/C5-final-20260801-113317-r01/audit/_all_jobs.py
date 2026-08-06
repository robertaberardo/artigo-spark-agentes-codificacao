# === /d/runs/C5-final-20260801-113317-r01/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
from pyspark.sql import types as T, functions as F

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

ID_WIDTH = 8

# Schema aplicado por posição às colunas do CSV (header é descartado); os nomes
# já entram em snake_case, a padronização de caixa do bronze.
RAW_SCHEMA = T.StructType(
    [
        T.StructField("id_unidade", T.StringType()),
        T.StructField("nome_unidade", T.StringType()),
        T.StructField("uf", T.StringType()),
        T.StructField("cod_uf", T.StringType()),
        T.StructField("nome_municipio", T.StringType()),
        T.StructField("cod_municipio", T.StringType()),
        T.StructField("setor", T.StringType()),
        T.StructField("area", T.StringType()),
        T.StructField("situacao", T.StringType()),
        T.StructField("latitude", T.StringType()),
        T.StructField("longitude", T.StringType()),
        T.StructField("qt_salas", T.StringType()),
        T.StructField("tem_agua", T.StringType()),
        T.StructField("tem_energia", T.StringType()),
        T.StructField("tem_esgoto", T.StringType()),
        T.StructField("tem_banheiro", T.StringType()),
        T.StructField("tem_biblioteca", T.StringType()),
        T.StructField("tem_lab_info", T.StringType()),
        T.StructField("tem_quadra", T.StringType()),
        T.StructField("tem_internet", T.StringType()),
    ]
)

INDICATORS = [
    "tem_agua", "tem_energia", "tem_esgoto", "tem_banheiro",
    "tem_biblioteca", "tem_lab_info", "tem_quadra", "tem_internet",
]


def main():
    """Lê o raw das escolas, padroniza nomes/tipos e grava a bronze em parquet."""
    spark = SparkSession.builder.appName("educacao-escola-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(F.col("id_unidade"), ID_WIDTH).alias("id_unidade"),
        ct.normalize_text(F.col("nome_unidade")).alias("nome_unidade"),
        ct.standardize_uf(F.col("uf")).alias("uf"),
        ct.digits_only(F.col("cod_uf")).alias("cod_uf"),
        ct.normalize_text(F.col("nome_municipio")).alias("nome_municipio"),
        ct.digits_only(F.col("cod_municipio")).alias("cod_municipio"),
        F.col("setor").cast("int").alias("setor"),
        F.col("area").cast("int").alias("area"),
        F.col("situacao").cast("int").alias("situacao"),
        ct.to_coordinate(F.col("latitude")).alias("latitude"),
        ct.to_coordinate(F.col("longitude")).alias("longitude"),
        F.col("qt_salas").cast("int").alias("qt_salas"),
        *[F.col(c).cast("int").alias(c) for c in INDICATORS],
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-113317-r01/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
from pyspark.sql import SparkSession
from pyspark.sql import types as T, functions as F

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

ID_WIDTH = 8

RAW_SCHEMA = T.StructType(
    [
        T.StructField("id_unidade", T.StringType()),
        T.StructField("mat_basica", T.StringType()),
        T.StructField("mat_infantil", T.StringType()),
        T.StructField("mat_fundamental", T.StringType()),
        T.StructField("mat_medio", T.StringType()),
        T.StructField("mat_profissional", T.StringType()),
        T.StructField("mat_eja", T.StringType()),
        T.StructField("mat_especial", T.StringType()),
    ]
)

# Colunas de contagem de matrícula: ausência fica NULL (não é zero).
COUNT_COLUMNS = [
    "mat_basica", "mat_infantil", "mat_fundamental", "mat_medio",
    "mat_profissional", "mat_eja", "mat_especial",
]


def main():
    """Lê o raw das matrículas, tipa as contagens e grava a bronze em parquet."""
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(F.col("id_unidade"), ID_WIDTH).alias("id_unidade"),
        *[F.col(c).cast("int").alias(c) for c in COUNT_COLUMNS],
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-113317-r01/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_functions as stf

from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
K_RING = 1  # disco de raio 1: a própria célula + as imediatamente adjacentes
SETOR_FEDERAL = 1
SITUACAO_ATIVA = 1
METERS_PER_KM = 1000.0


def build_federal_schools(geo: DataFrame) -> DataFrame:
    """Filtra escolas federais em atividade e anexa a célula H3 nível 5.

    Universo de análise: só ``setor`` federal e ``situacao`` em atividade. O H3 é
    calculado sobre o ponto WGS84 original (``ST_H3CellIDs`` opera em graus).
    """
    is_federal = F.col("setor") == SETOR_FEDERAL
    is_active = F.col("situacao") == SITUACAO_ATIVA
    return (
        geo.filter(is_federal & is_active)
        .withColumn(
            "h3_cell",
            F.element_at(stf.ST_H3CellIDs(F.col("geometry"), H3_LEVEL, False), 1),
        )
        .select("id_unidade", "enrollment", "geometry", "h3_cell")
    )


def cell_base_metrics(schools: DataFrame) -> DataFrame:
    """Contagem de escolas, total e média de matrículas por célula."""
    return schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("enrollment").cast("long").alias("total_enrollment"),
        F.avg("enrollment").alias("avg_enrollment"),
    )


def nearest_federal_per_cell(schools: DataFrame) -> DataFrame:
    """Distância média (km) à federal mais próxima, sem restrição de célula.

    Para cada escola mede a distância geodésica à federal em atividade mais
    próxima (qualquer, em todo o país) e devolve a média dessas distâncias por
    célula.
    """
    left = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("geometry").alias("a_geom"),
    )
    right = schools.select(
        F.col("id_unidade").alias("b_id"),
        F.col("geometry").alias("b_geom"),
    )
    pairs = left.crossJoin(F.broadcast(right)).filter(F.col("a_id") != F.col("b_id"))
    dist_km = stf.ST_DistanceSpheroid(F.col("a_geom"), F.col("b_geom")) / METERS_PER_KM

    nearest = pairs.withColumn("dist_km", dist_km).groupBy("a_id", "a_cell").agg(
        F.min("dist_km").alias("nearest_km")
    )
    return nearest.groupBy("a_cell").agg(
        F.avg("nearest_km").alias("avg_dist_nearest_km")
    ).select(F.col("a_cell").alias("h3_cell"), F.col("avg_dist_nearest_km"))


def neighbor_metrics_per_cell(schools: DataFrame) -> DataFrame:
    """Vizinhas no disco H3: contagem por célula e distância média às vizinhas.

    Vizinhas de uma escola são as outras federais em atividade na própria célula
    ou nas adjacentes (disco ``ST_H3KRing`` de raio 1). A contagem é a mesma para
    toda a célula (todas partilham o disco); ``avg_dist_neighbors_km`` é a média,
    na célula, da distância geodésica média de cada escola às suas vizinhas.
    """
    left = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("geometry").alias("a_geom"),
    )
    disk = left.withColumn(
        "disk_cell", F.explode(stf.ST_H3KRing(F.col("a_cell"), K_RING, False))
    )
    right = schools.select(
        F.col("id_unidade").alias("b_id"),
        F.col("h3_cell").alias("b_cell"),
        F.col("geometry").alias("b_geom"),
    )
    same_cell = F.col("disk_cell") == F.col("b_cell")
    not_self = F.col("a_id") != F.col("b_id")
    dist_km = stf.ST_DistanceSpheroid(F.col("a_geom"), F.col("b_geom")) / METERS_PER_KM

    pairs = (
        disk.join(F.broadcast(right), same_cell, "inner")
        .filter(not_self)
        .withColumn("dist_km", dist_km)
    )
    per_school = pairs.groupBy("a_id", "a_cell").agg(
        F.avg("dist_km").alias("mean_nb_km"),
        F.count(F.lit(1)).cast("long").alias("n_nb"),
    )

    # Escolas sem vizinhas somem no inner join; reincorpora-as com n_nb=0 para não
    # sumir a célula nem enviesar a contagem.
    all_schools = schools.select(
        F.col("id_unidade").alias("a_id"), F.col("h3_cell").alias("a_cell")
    )
    complete = all_schools.join(per_school, ["a_id", "a_cell"], "left").select(
        F.col("a_cell"),
        F.col("mean_nb_km"),
        F.coalesce(F.col("n_nb"), F.lit(0)).alias("n_nb"),
    )
    return complete.groupBy("a_cell").agg(
        F.avg("mean_nb_km").alias("avg_dist_neighbors_km"),
        F.max("n_nb").alias("n_federal_neighbors"),
    ).select(
        F.col("a_cell").alias("h3_cell"),
        F.col("avg_dist_neighbors_km"),
        F.col("n_federal_neighbors"),
    )


def main():
    """Grade H3 nível 5 das escolas federais em atividade com matrículas e vizinhança."""
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    geo = spark.read.format("geoparquet").load(ORIGEM)
    schools = build_federal_schools(geo).cache()

    base = cell_base_metrics(schools)
    neighbors = neighbor_metrics_per_cell(schools)
    nearest = nearest_federal_per_cell(schools)

    grid = base.join(neighbors, "h3_cell", "left").join(nearest, "h3_cell", "left")

    result = grid.withColumn(
        "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
    ).select(
        F.col("h3_cell"),
        F.col("n_schools"),
        F.col("total_enrollment"),
        F.round(F.col("avg_enrollment"), 2).alias("avg_enrollment"),
        F.col("n_federal_neighbors").cast("long").alias("n_federal_neighbors"),
        F.round(F.col("avg_dist_neighbors_km"), 3).alias("avg_dist_neighbors_km"),
        F.round(F.col("avg_dist_nearest_km"), 3).alias("avg_dist_nearest_km"),
        F.col("geometry"),
    )

    result.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-113317-r01/workdir/apps/educacao/jobs/silver/educacao_escola_geo.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_geo", "silver")

# A geometria fica em WGS84 (EPSG:4326): o gold indexa H3 e mede distância
# geodésica sobre as coordenadas originais, sem round-trip por 3857.


def main():
    """Escolas georreferenciadas (ponto WGS84) enriquecidas com as matrículas."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA).select(
        F.col("id_unidade"),
        F.col("mat_basica").alias("enrollment"),
    )

    escola_valida = escola.transform(
        lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude")
    )

    enriquecida = dt.join_no_fanout(
        escola_valida, matricula, on="id_unidade", how="left"
    )

    geo = (
        enriquecida.transform(
            lambda d: dt.add_point_geometry(d, "latitude", "longitude")
        ).select(
            F.col("id_unidade"),
            F.col("uf"),
            F.col("setor"),
            F.col("situacao"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("enrollment"),
            F.col("geometry"),
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

