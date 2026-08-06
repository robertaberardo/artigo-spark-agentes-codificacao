# === /d/runs/C5-final-20260731-235945-r08/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# O raw aterrissa como texto separado por ';'; tudo entra como string e o
# saneamento/tipagem acontece já no select do bronze.
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
    """Lê o raw das escolas, padroniza nomes/tipos e grava a bronze."""
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
        ct.blank_to_null(raw["SETOR"]).cast("int").alias("setor"),
        ct.blank_to_null(raw["AREA"]).cast("int").alias("area"),
        ct.blank_to_null(raw["SITUACAO"]).cast("int").alias("situacao"),
        ct.to_coordinate(raw["LATITUDE"]).alias("latitude"),
        ct.to_coordinate(raw["LONGITUDE"]).alias("longitude"),
        ct.blank_to_null(raw["QT_SALAS"]).cast("int").alias("qt_salas"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r08/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

# Contagens de matrícula aterrissam como texto; entram como string e viram int
# no select. Ausência permanece NULL (nunca zero): matrícula ausente não é zero.
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
    """Lê o raw das matrículas, padroniza nomes/tipos e grava a bronze."""
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
        ct.blank_to_null(raw["MAT_BASICA"]).cast("int").alias("mat_basica"),
        ct.blank_to_null(raw["MAT_INFANTIL"]).cast("int").alias("mat_infantil"),
        ct.blank_to_null(raw["MAT_FUNDAMENTAL"]).cast("int").alias("mat_fundamental"),
        ct.blank_to_null(raw["MAT_MEDIO"]).cast("int").alias("mat_medio"),
        ct.blank_to_null(raw["MAT_PROFISSIONAL"]).cast("int").alias("mat_profissional"),
        ct.blank_to_null(raw["MAT_EJA"]).cast("int").alias("mat_eja"),
        ct.blank_to_null(raw["MAT_ESPECIAL"]).cast("int").alias("mat_especial"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r08/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
FEDERAL = 1  # setor administrativo federal
ACTIVE = 1  # situação "em atividade"
METERS_PER_KM = 1000.0


def prepare_federal_active_schools(silver: DataFrame) -> DataFrame:
    """Filtra federais em atividade e anexa ponto WGS84 + célula H3 nível 5.

    O ponto é reconstruído a partir das lat/long originais (WGS84) para que as
    distâncias sejam geodésicas sobre as coordenadas originais, e o índice H3 é
    calculado sobre esse mesmo ponto (o H3 opera em graus).
    """
    is_federal = F.col("setor") == FEDERAL
    is_active = F.col("situacao") == ACTIVE
    return (
        silver.filter(is_federal & is_active)
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude", "geom_wgs"))
        .transform(lambda d: dt.attach_h3_index(d, "geom_wgs", H3_LEVEL, "h3_cell"))
        .select("id_unidade", "h3_cell", "geom_wgs", "total_enrollment")
    )


def aggregate_cell_base(schools: DataFrame) -> DataFrame:
    """Contagem de escolas, total e média de matrículas por célula.

    ``avg`` e ``sum`` ignoram nulos (matrícula ausente não é zero), então a média
    considera apenas escolas com matrícula informada.
    """
    return schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("total_enrollment").cast("long").alias("total_enrollment"),
        F.avg("total_enrollment").alias("avg_enrollment"),
    )


def count_federal_neighbors_per_cell(schools: DataFrame) -> DataFrame:
    """Nº de federais em atividade na célula + adjacentes (mesmo p/ toda a célula).

    Soma, para cada célula-alvo, as escolas contidas nela e em cada célula do seu
    anel-1 (``ST_H3KRing`` com ``k=1``), formando o tamanho do pool de vizinhança.
    """
    cell_counts = schools.groupBy("h3_cell").agg(F.count(F.lit(1)).alias("n_in_cell"))
    target = cell_counts.select(
        F.col("h3_cell").alias("target_cell"),
        F.explode(stf.ST_H3KRing(F.col("h3_cell"), 1, False)).alias("member_cell"),
    )
    return (
        target.join(
            cell_counts, target["member_cell"] == cell_counts["h3_cell"], how="left"
        )
        .groupBy("target_cell")
        .agg(F.sum("n_in_cell").cast("long").alias("n_federal_neighbors"))
        .select(F.col("target_cell").alias("h3_cell"), "n_federal_neighbors")
    )


def average_neighbor_distance_per_cell(schools: DataFrame) -> DataFrame:
    """Distância geodésica média de cada escola às suas vizinhas, média por célula.

    Vizinhas = outras federais em atividade na própria célula ou nas adjacentes
    (anel-1). Mede-se a distância por escola e depois a média entre as escolas da
    célula.
    """
    left = schools.select(
        F.col("id_unidade").alias("id_a"),
        F.col("h3_cell").alias("cell_a"),
        F.col("geom_wgs").alias("geom_a"),
    ).withColumn("ring_cell", F.explode(stf.ST_H3KRing(F.col("cell_a"), 1, False)))
    right = schools.select(
        F.col("id_unidade").alias("id_b"),
        F.col("h3_cell").alias("cell_b"),
        F.col("geom_wgs").alias("geom_b"),
    )
    pairs = left.join(
        right, F.col("ring_cell") == F.col("cell_b"), how="inner"
    ).filter(F.col("id_a") != F.col("id_b"))
    per_school = (
        pairs.withColumn(
            "dist_m", stf.ST_DistanceSpheroid(F.col("geom_a"), F.col("geom_b"))
        )
        .groupBy("id_a", "cell_a")
        .agg(F.avg("dist_m").alias("neighbor_mean_m"))
    )
    return (
        per_school.groupBy("cell_a")
        .agg(F.avg("neighbor_mean_m").alias("avg_dist_neighbors_m"))
        .select(F.col("cell_a").alias("h3_cell"), "avg_dist_neighbors_m")
    )


def average_nearest_distance_per_cell(schools: DataFrame) -> DataFrame:
    """Distância à federal mais próxima (qualquer célula), média por célula.

    Para cada escola, a menor distância geodésica a qualquer outra federal em
    atividade (sem restrição de célula); depois a média entre as escolas da célula.
    """
    left = schools.select(
        F.col("id_unidade").alias("id_a"),
        F.col("h3_cell").alias("cell_a"),
        F.col("geom_wgs").alias("geom_a"),
    )
    right = schools.select(
        F.col("id_unidade").alias("id_b"), F.col("geom_wgs").alias("geom_b")
    )
    pairs = left.crossJoin(right).filter(F.col("id_a") != F.col("id_b"))
    per_school = (
        pairs.withColumn(
            "dist_m", stf.ST_DistanceSpheroid(F.col("geom_a"), F.col("geom_b"))
        )
        .groupBy("id_a", "cell_a")
        .agg(F.min("dist_m").alias("nearest_m"))
    )
    return (
        per_school.groupBy("cell_a")
        .agg(F.avg("nearest_m").alias("avg_dist_nearest_m"))
        .select(F.col("cell_a").alias("h3_cell"), "avg_dist_nearest_m")
    )


def build_grid(schools: DataFrame) -> DataFrame:
    """Junta as métricas por célula e monta o schema final com a geometria H3."""
    base = aggregate_cell_base(schools)
    neighbors_count = count_federal_neighbors_per_cell(schools)
    neighbors_dist = average_neighbor_distance_per_cell(schools)
    nearest_dist = average_nearest_distance_per_cell(schools)

    return (
        base.join(neighbors_count, on="h3_cell", how="left")
        .join(neighbors_dist, on="h3_cell", how="left")
        .join(nearest_dist, on="h3_cell", how="left")
        .select(
            F.col("h3_cell"),
            F.col("n_schools"),
            F.col("total_enrollment"),
            F.round(F.col("avg_enrollment"), 2).alias("avg_enrollment"),
            F.col("n_federal_neighbors"),
            F.round(F.col("avg_dist_neighbors_m") / METERS_PER_KM, 3).alias(
                "avg_dist_neighbors_km"
            ),
            F.round(F.col("avg_dist_nearest_m") / METERS_PER_KM, 3).alias(
                "avg_dist_nearest_km"
            ),
            F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1).alias("geometry"),
        )
    )


def main():
    """Grade H3 (nível 5) de escolas federais em atividade + matrículas + vizinhança."""
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("geoparquet").load(ORIGEM)
    schools = prepare_federal_active_schools(silver).cache()

    grid = build_grid(schools)

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r08/workdir/apps/educacao/jobs/silver/educacao_escola_geo.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_geo", "silver")


def main():
    """Escolas georreferenciadas com matrículas: ponto WGS84 + geometria 3857.

    Mantém latitude/longitude originais para que a camada gold meça distâncias
    geodésicas sobre o WGS84 antes de qualquer projeção; a geometria gravada vai
    em EPSG:3857 (convenção de saída do lake).
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA).select(
        F.col("id_unidade"), F.col("mat_basica").alias("total_enrollment")
    )

    geo = (
        escola.transform(
            lambda d: dt.join_no_fanout(d, matricula, "id_unidade", how="left")
        )
        .transform(
            lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude")
        )
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "uf",
            "setor",
            "situacao",
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

