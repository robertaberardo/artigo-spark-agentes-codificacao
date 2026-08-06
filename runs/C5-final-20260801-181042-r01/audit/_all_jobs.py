# === /d/runs/C5-final-20260801-181042-r01/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# Colunas do CSV cru, na ordem e caixa da origem (Censo Escolar).
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


def main():
    """Lê o raw das escolas, tipa colunas (lat/long double) e salva a bronze."""
    spark = SparkSession.builder.appName("educacao-escola-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(raw["ID_UNIDADE"], 8).alias("id_unidade"),
        ct.normalize_text(raw["NOME_UNIDADE"]).alias("nome_unidade"),
        ct.standardize_uf(raw["UF"]).alias("uf"),
        ct.digits_only(raw["COD_MUNICIPIO"]).alias("cod_municipio"),
        raw["SETOR"].cast("int").alias("setor"),
        raw["SITUACAO"].cast("int").alias("situacao"),
        ct.to_coordinate(raw["LATITUDE"]).alias("latitude"),
        ct.to_coordinate(raw["LONGITUDE"]).alias("longitude"),
        raw["QT_SALAS"].cast("int").alias("qt_salas"),
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-181042-r01/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

# Colunas do CSV cru, na ordem e caixa da origem (Censo Escolar).
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


def main():
    """Lê o raw das matrículas, tipa as contagens (int) e salva a bronze."""
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(raw["ID_UNIDADE"], 8).alias("id_unidade"),
        raw["MAT_BASICA"].cast("int").alias("mat_basica"),
        raw["MAT_INFANTIL"].cast("int").alias("mat_infantil"),
        raw["MAT_FUNDAMENTAL"].cast("int").alias("mat_fundamental"),
        raw["MAT_MEDIO"].cast("int").alias("mat_medio"),
        raw["MAT_PROFISSIONAL"].cast("int").alias("mat_profissional"),
        raw["MAT_EJA"].cast("int").alias("mat_eja"),
        raw["MAT_ESPECIAL"].cast("int").alias("mat_especial"),
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-181042-r01/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
# k=1 cobre a própria célula e as imediatamente adjacentes (7 células).
NEIGHBOR_K = 1
METERS_PER_KM = 1000.0


def prepare_schools(df: DataFrame) -> DataFrame:
    """Reconstrói o ponto WGS84 original e indexa cada escola na célula H3 nível 5.

    O ponto vem de lat/long originais (não da geometria 3857 da silver): as
    distâncias geodésicas são medidas sobre o WGS84 original, sem o round-trip
    de reprojeção. O H3 também opera em coordenadas geográficas.
    """
    point = stc.ST_Point(F.col("longitude"), F.col("latitude"))
    return (
        df.withColumn("point", point)
        .withColumn(
            "h3_cell", F.element_at(stf.ST_H3CellIDs(F.col("point"), H3_LEVEL, False), 1)
        )
        .select("id_unidade", "h3_cell", "total_enrollment", "point")
    )


def base_metrics(schools: DataFrame) -> DataFrame:
    """Contagem, total e média de matrículas por célula."""
    return schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("total_enrollment").cast("long").alias("total_enrollment"),
        F.avg("total_enrollment").alias("avg_enrollment"),
    )


def neighbor_counts(schools: DataFrame) -> DataFrame:
    """Nº de escolas federais na célula e nas adjacentes (igual para toda a célula).

    Expande cada célula no seu k-ring (própria + adjacentes) e soma as escolas de
    cada célula do anel. Anéis sem escolas não contribuem (``left join``).
    """
    cell_counts = schools.groupBy("h3_cell").agg(F.count(F.lit(1)).alias("cell_n"))
    rings = cell_counts.select(
        "h3_cell",
        F.explode(
            stf.ST_H3KRing(F.col("h3_cell"), F.lit(NEIGHBOR_K), F.lit(False))
        ).alias("ring_cell"),
    )
    counts_by_ring = cell_counts.select(
        F.col("h3_cell").alias("ring_cell"), F.col("cell_n")
    )
    return (
        rings.join(counts_by_ring, on="ring_cell", how="left")
        .groupBy("h3_cell")
        .agg(F.sum("cell_n").cast("long").alias("n_federal_neighbors"))
    )


def neighbor_distances(schools: DataFrame) -> DataFrame:
    """Distância geodésica média de cada escola às suas vizinhas, média na célula.

    Vizinhas = outras escolas cuja célula está no k-ring da escola (própria célula
    ou adjacentes). Mede em WGS84 (``ST_DistanceSpheroid``) e converte para km.
    """
    origins = schools.select(
        F.col("id_unidade").alias("origin_id"),
        F.col("h3_cell").alias("origin_cell"),
        F.col("point").alias("origin_point"),
    ).withColumn(
        "ring_cell",
        F.explode(stf.ST_H3KRing(F.col("origin_cell"), F.lit(NEIGHBOR_K), F.lit(False))),
    )
    candidates = schools.select(
        F.col("id_unidade").alias("neighbor_id"),
        F.col("h3_cell").alias("ring_cell"),
        F.col("point").alias("neighbor_point"),
    )
    dist_km = (
        stf.ST_DistanceSpheroid(F.col("origin_point"), F.col("neighbor_point"))
        / METERS_PER_KM
    )
    pairs = (
        origins.join(candidates, on="ring_cell", how="inner")
        .filter(F.col("origin_id") != F.col("neighbor_id"))
        .withColumn("dist_km", dist_km)
    )
    per_school = pairs.groupBy("origin_cell", "origin_id").agg(
        F.avg("dist_km").alias("school_avg_dist")
    )
    return per_school.groupBy(F.col("origin_cell").alias("h3_cell")).agg(
        F.avg("school_avg_dist").alias("avg_dist_neighbors_km")
    )


def nearest_distances(schools: DataFrame) -> DataFrame:
    """Distância geodésica média de cada escola à federal mais próxima (qualquer).

    Sem restrição de célula: para cada escola, a menor distância a qualquer outra
    escola federal; depois a média na célula.
    """
    origins = schools.select(
        F.col("id_unidade").alias("origin_id"),
        F.col("h3_cell").alias("origin_cell"),
        F.col("point").alias("origin_point"),
    )
    others = schools.select(
        F.col("id_unidade").alias("other_id"),
        F.col("point").alias("other_point"),
    )
    dist_km = (
        stf.ST_DistanceSpheroid(F.col("origin_point"), F.col("other_point"))
        / METERS_PER_KM
    )
    pairs = (
        origins.crossJoin(others)
        .filter(F.col("origin_id") != F.col("other_id"))
        .withColumn("dist_km", dist_km)
    )
    per_school = pairs.groupBy("origin_cell", "origin_id").agg(
        F.min("dist_km").alias("school_nearest")
    )
    return per_school.groupBy(F.col("origin_cell").alias("h3_cell")).agg(
        F.avg("school_nearest").alias("avg_dist_nearest_km")
    )


def build_grid(schools: DataFrame) -> DataFrame:
    """Junta as métricas por célula e reconstrói o polígono H3."""
    grid = (
        base_metrics(schools)
        .join(neighbor_counts(schools), on="h3_cell", how="left")
        .join(neighbor_distances(schools), on="h3_cell", how="left")
        .join(nearest_distances(schools), on="h3_cell", how="left")
    )
    cell_geom = F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
    return grid.select(
        F.col("h3_cell"),
        F.col("n_schools"),
        F.col("total_enrollment"),
        F.round(F.col("avg_enrollment"), 2).alias("avg_enrollment"),
        F.col("n_federal_neighbors"),
        F.round(F.col("avg_dist_neighbors_km"), 3).alias("avg_dist_neighbors_km"),
        F.round(F.col("avg_dist_nearest_km"), 3).alias("avg_dist_nearest_km"),
        cell_geom.alias("geometry"),
    )


def main():
    """Grade H3 nível 5 de escolas federais em atividade (densidade + distâncias)."""
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = spark.read.format("geoparquet").load(ORIGEM).transform(prepare_schools)
    # Reutilizado por quatro agregações independentes; materializa uma vez.
    schools.cache()

    grid = build_grid(schools)

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-181042-r01/workdir/apps/educacao/jobs/silver/educacao_escola_geo.py ===
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_geo", "silver")

# Códigos do Censo Escolar filtrados nesta silver.
SETOR_FEDERAL = 1
SITUACAO_EM_ATIVIDADE = 1


def keep_federal_active(df: DataFrame) -> DataFrame:
    """Mantém apenas escolas federais em atividade."""
    is_federal = F.col("setor") == SETOR_FEDERAL
    is_active = F.col("situacao") == SITUACAO_EM_ATIVIDADE
    return df.filter(is_federal & is_active)


def attach_enrollment(escola: DataFrame, matricula: DataFrame) -> DataFrame:
    """Anexa o total de matrículas da educação básica de cada escola.

    ``left join`` pela chave da unidade: escolas sem linha de matrícula ficam
    com ``total_enrollment`` nulo (ausência não é zero).
    """
    enrollment = matricula.select(
        F.col("id_unidade"),
        F.col("mat_basica").alias("total_enrollment"),
    )
    return escola.join(enrollment, on="id_unidade", how="left")


def main():
    """Escolas federais em atividade georreferenciadas (ponto, matrículas, 3857)."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA).transform(keep_federal_active)
    matricula = spark.read.parquet(MATRICULA)

    geo = (
        attach_enrollment(escola, matricula)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        # Preserva lat/long WGS84 originais: as distâncias geodésicas do gold são
        # medidas sobre o ponto original, antes de qualquer reprojeção (evita o
        # round-trip 4326→3857→4326). A geometria persistida vai para 3857.
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "uf",
            "cod_municipio",
            "latitude",
            "longitude",
            "total_enrollment",
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

