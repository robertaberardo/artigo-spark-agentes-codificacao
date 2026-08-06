# === /d/runs/C5-final-20260801-100109-r01/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("ID_UNIDADE", StringType()),
        StructField("NOME_UNIDADE", StringType()),
        StructField("UF", StringType()),
        StructField("COD_UF", StringType()),
        StructField("NOME_MUNICIPIO", StringType()),
        StructField("COD_MUNICIPIO", StringType()),
        StructField("SETOR", StringType()),
        StructField("AREA", StringType()),
        StructField("SITUACAO", StringType()),
        StructField("LATITUDE", StringType()),
        StructField("LONGITUDE", StringType()),
        StructField("QT_SALAS", StringType()),
        StructField("TEM_AGUA", StringType()),
        StructField("TEM_ENERGIA", StringType()),
        StructField("TEM_ESGOTO", StringType()),
        StructField("TEM_BANHEIRO", StringType()),
        StructField("TEM_BIBLIOTECA", StringType()),
        StructField("TEM_LAB_INFO", StringType()),
        StructField("TEM_QUADRA", StringType()),
        StructField("TEM_INTERNET", StringType()),
    ]
)

# Indicadores 0/1 de infraestrutura: mesmo tratamento (texto -> int) para todos.
INFRA_COLS = [
    "TEM_AGUA",
    "TEM_ENERGIA",
    "TEM_ESGOTO",
    "TEM_BANHEIRO",
    "TEM_BIBLIOTECA",
    "TEM_LAB_INFO",
    "TEM_QUADRA",
    "TEM_INTERNET",
]

ID_WIDTH = 8


def main():
    """Lê o raw das escolas, tipa/limpa as colunas e grava a bronze."""
    spark = SparkSession.builder.appName("educacao-escola-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    infra = [
        F.col(c).cast("int").alias(c.lower()) for c in INFRA_COLS
    ]
    bronze = raw.select(
        ct.zero_pad_code(raw["ID_UNIDADE"], ID_WIDTH).alias("id_unidade"),
        ct.normalize_text(raw["NOME_UNIDADE"]).alias("nome_unidade"),
        ct.standardize_uf(raw["UF"]).alias("uf"),
        ct.digits_only(raw["COD_UF"]).alias("cod_uf"),
        ct.normalize_text(raw["NOME_MUNICIPIO"]).alias("nome_municipio"),
        ct.digits_only(raw["COD_MUNICIPIO"]).alias("cod_municipio"),
        raw["SETOR"].cast("int").alias("setor"),
        raw["AREA"].cast("int").alias("area"),
        raw["SITUACAO"].cast("int").alias("situacao"),
        ct.to_coordinate(raw["LATITUDE"]).alias("latitude"),
        ct.to_coordinate(raw["LONGITUDE"]).alias("longitude"),
        raw["QT_SALAS"].cast("int").alias("qt_salas"),
        *infra,
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-100109-r01/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

RAW_SCHEMA = StructType(
    [
        StructField("ID_UNIDADE", StringType()),
        StructField("MAT_BASICA", StringType()),
        StructField("MAT_INFANTIL", StringType()),
        StructField("MAT_FUNDAMENTAL", StringType()),
        StructField("MAT_MEDIO", StringType()),
        StructField("MAT_PROFISSIONAL", StringType()),
        StructField("MAT_EJA", StringType()),
        StructField("MAT_ESPECIAL", StringType()),
    ]
)

# Colunas de contagem de matrículas: texto -> int, mesma limpeza.
MAT_COLS = [
    "MAT_BASICA",
    "MAT_INFANTIL",
    "MAT_FUNDAMENTAL",
    "MAT_MEDIO",
    "MAT_PROFISSIONAL",
    "MAT_EJA",
    "MAT_ESPECIAL",
]

ID_WIDTH = 8


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

    counts = [F.col(c).cast("int").alias(c.lower()) for c in MAT_COLS]
    bronze = raw.select(
        ct.zero_pad_code(raw["ID_UNIDADE"], ID_WIDTH).alias("id_unidade"),
        *counts,
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-100109-r01/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
KRING = 1  # anel 1: a própria célula e as imediatamente adjacentes
METERS_PER_KM = 1000.0
FEDERAL, ACTIVE = 1, 1


def select_federal_active(silver: DataFrame) -> DataFrame:
    """Escolas federais em atividade, com ponto WGS84 e célula H3 nível 5.

    O ponto é reconstruído a partir de latitude/longitude WGS84 originais (não da
    geometria 3857) para que as distâncias geodésicas sejam medidas sobre as
    coordenadas de origem, sem round-trip de projeção.
    """
    is_federal = F.col("setor") == FEDERAL
    is_active = F.col("situacao") == ACTIVE
    point = stc.ST_Point(F.col("longitude"), F.col("latitude"))
    return (
        silver.filter(is_federal & is_active)
        .withColumn("point", point)
        .withColumn(
            "h3_cell", F.element_at(stf.ST_H3CellIDs(F.col("point"), H3_LEVEL, False), 1)
        )
        .select("id_unidade", "total_enrollment", "h3_cell", "point")
    )


def base_metrics_by_cell(schools: DataFrame) -> DataFrame:
    """Contagem de escolas, total e média de matrículas por célula.

    ``avg``/``sum`` ignoram nulos: escolas sem registro de matrícula não entram
    como zero (ausência não é zero).
    """
    return schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("total_enrollment").cast("long").alias("total_enrollment"),
        F.round(F.avg("total_enrollment"), 2).alias("avg_enrollment"),
    )


def _neighbor_pairs(schools: DataFrame) -> DataFrame:
    """Pares (s, t) de escolas cujas células distam no máximo um anel H3.

    Explode o k-ring (k=1, que inclui a própria célula) de cada escola s e casa
    com as escolas t situadas nessas células; descarta o par consigo mesma. A
    distância ``dist_m`` é geodésica (``ST_DistanceSpheroid``, WGS84).
    """
    ring = schools.select(
        F.col("id_unidade").alias("s_id"),
        F.col("h3_cell").alias("s_cell"),
        F.col("point").alias("s_point"),
        F.explode(stf.ST_H3KRing(F.col("h3_cell"), KRING, False)).alias("ring_cell"),
    )
    others = schools.select(
        F.col("id_unidade").alias("t_id"),
        F.col("h3_cell").alias("t_cell"),
        F.col("point").alias("t_point"),
    )
    pairs = ring.join(others, ring["ring_cell"] == others["t_cell"], "inner")
    return pairs.filter(F.col("s_id") != F.col("t_id")).withColumn(
        "dist_m", stf.ST_DistanceSpheroid(F.col("s_point"), F.col("t_point"))
    )


def neighbor_distance_by_cell(schools: DataFrame) -> DataFrame:
    """Distância média (km) de cada escola às suas vizinhas, agregada por célula.

    Primeiro a média por escola (distância às vizinhas na célula + adjacentes),
    depois a média dessas médias entre as escolas da célula.
    """
    per_school = (
        _neighbor_pairs(schools)
        .groupBy("s_id", "s_cell")
        .agg(F.avg("dist_m").alias("school_avg_m"))
    )
    return (
        per_school.groupBy("s_cell")
        .agg(F.round(F.avg("school_avg_m") / METERS_PER_KM, 3).alias("avg_dist_neighbors_km"))
        .select(F.col("s_cell").alias("h3_cell"), "avg_dist_neighbors_km")
    )


def neighbor_count_by_cell(schools: DataFrame) -> DataFrame:
    """Número de escolas federais na célula e nas adjacentes (constante por célula).

    Soma as contagens por célula sobre o k-ring (k=1, inclui a própria célula) de
    cada célula ocupada.
    """
    cell_counts = schools.groupBy("h3_cell").agg(F.count(F.lit(1)).alias("cell_n"))
    rings = schools.select("h3_cell").distinct().select(
        F.col("h3_cell").alias("base_cell"),
        F.explode(stf.ST_H3KRing(F.col("h3_cell"), KRING, False)).alias("ring_cell"),
    )
    joined = rings.join(cell_counts, rings["ring_cell"] == cell_counts["h3_cell"], "inner")
    return (
        joined.groupBy("base_cell")
        .agg(F.sum("cell_n").cast("long").alias("n_federal_neighbors"))
        .select(F.col("base_cell").alias("h3_cell"), "n_federal_neighbors")
    )


def nearest_distance_by_cell(schools: DataFrame) -> DataFrame:
    """Distância média (km) de cada escola à federal mais próxima, por célula.

    Sem restrição de célula: para cada escola, a menor distância geodésica a
    qualquer outra escola federal em atividade; depois a média por célula. O lado
    direito (poucas centenas de escolas) é pequeno o bastante para broadcast.
    """
    left = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("point").alias("a_point"),
    )
    right = schools.select(
        F.col("id_unidade").alias("b_id"),
        F.col("point").alias("b_point"),
    )
    pairs = left.crossJoin(F.broadcast(right)).filter(F.col("a_id") != F.col("b_id"))
    nearest = (
        pairs.withColumn(
            "dist_m", stf.ST_DistanceSpheroid(F.col("a_point"), F.col("b_point"))
        )
        .groupBy("a_id", "a_cell")
        .agg(F.min("dist_m").alias("nearest_m"))
    )
    return (
        nearest.groupBy("a_cell")
        .agg(F.round(F.avg("nearest_m") / METERS_PER_KM, 3).alias("avg_dist_nearest_km"))
        .select(F.col("a_cell").alias("h3_cell"), "avg_dist_nearest_km")
    )


def main():
    """Grade H3 nível 5 das escolas federais em atividade, com matrículas,
    vizinhança e proximidade."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("geoparquet").load(ORIGEM)
    schools = select_federal_active(silver).cache()

    base = base_metrics_by_cell(schools)
    n_neighbors = neighbor_count_by_cell(schools)
    neighbor_dist = neighbor_distance_by_cell(schools)
    nearest_dist = nearest_distance_by_cell(schools)

    # Polígono da célula (H3 opera em WGS84); persiste em 3857 por convenção.
    cell_geometry = stf.ST_Transform(
        F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1),
        F.lit("EPSG:4326"),
        F.lit("EPSG:3857"),
    )
    grid = (
        base.join(n_neighbors, on="h3_cell", how="left")
        .join(neighbor_dist, on="h3_cell", how="left")
        .join(nearest_dist, on="h3_cell", how="left")
        .withColumn("geometry", cell_geometry)
        .select(
            "h3_cell",
            "n_schools",
            "total_enrollment",
            "avg_enrollment",
            "n_federal_neighbors",
            "avg_dist_neighbors_km",
            "avg_dist_nearest_km",
            "geometry",
        )
    )

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-100109-r01/workdir/apps/educacao/jobs/silver/educacao_escola_geo.py ===
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_geo", "silver")


def attach_enrollment(escola: DataFrame, matricula: DataFrame) -> DataFrame:
    """Anexa o total de matrículas (educação básica) por escola.

    ``left join`` para preservar toda escola georreferenciada, mesmo sem
    registro de matrícula — nesse caso ``total_enrollment`` fica ``NULL``
    (ausência não é zero). A chave ``id_unidade`` é única na matrícula, então o
    join não deve multiplicar linhas (``join_no_fanout`` valida).
    """
    enrollment = matricula.select(
        F.col("id_unidade"),
        F.col("mat_basica").alias("total_enrollment"),
    )
    return dt.join_no_fanout(escola, enrollment, on="id_unidade", how="left")


def main():
    """Escolas georreferenciadas com matrículas: ponto WGS84 e geometria 3857."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA)

    geo = (
        escola.transform(
            lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude")
        )
        .transform(lambda d: attach_enrollment(d, matricula))
        # latitude/longitude WGS84 originais permanecem no schema para a medida
        # geodésica no gold; só a geometria persistida vai para 3857.
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "nome_unidade",
            "uf",
            "cod_municipio",
            "setor",
            "situacao",
            "total_enrollment",
            "latitude",
            "longitude",
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

