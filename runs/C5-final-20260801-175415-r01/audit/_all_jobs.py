# === /d/runs/C5-final-20260801-175415-r01/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
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


def main():
    """Lê o raw das escolas, tipa/limpa as colunas em snake_case e salva a bronze."""
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
        ct.digits_only(raw["COD_UF"]).alias("cod_uf"),
        ct.normalize_text(raw["NOME_MUNICIPIO"]).alias("nome_municipio"),
        ct.digits_only(raw["COD_MUNICIPIO"]).alias("cod_municipio"),
        ct.blank_to_null(raw["SETOR"]).alias("setor"),
        ct.blank_to_null(raw["AREA"]).alias("area"),
        ct.blank_to_null(raw["SITUACAO"]).alias("situacao"),
        ct.to_coordinate(raw["LATITUDE"]).alias("latitude"),
        ct.to_coordinate(raw["LONGITUDE"]).alias("longitude"),
        raw["QT_SALAS"].cast("int").alias("qt_salas"),
        raw["TEM_AGUA"].cast("int").alias("tem_agua"),
        raw["TEM_ENERGIA"].cast("int").alias("tem_energia"),
        raw["TEM_ESGOTO"].cast("int").alias("tem_esgoto"),
        raw["TEM_BANHEIRO"].cast("int").alias("tem_banheiro"),
        raw["TEM_BIBLIOTECA"].cast("int").alias("tem_biblioteca"),
        raw["TEM_LAB_INFO"].cast("int").alias("tem_lab_info"),
        raw["TEM_QUADRA"].cast("int").alias("tem_quadra"),
        raw["TEM_INTERNET"].cast("int").alias("tem_internet"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-175415-r01/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
from pyspark.sql import SparkSession
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


def main():
    """Lê o raw das matrículas, tipa as colunas em snake_case e salva a bronze."""
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
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-175415-r01/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_federal_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
KM = 1_000.0


def load_schools(spark) -> DataFrame:
    """Escolas federais com ponto WGS84 (para geodésica) e célula H3 nível 5."""
    return (
        spark.read.format("geoparquet")
        .load(ORIGEM)
        .select("id_unidade", "total_enrollment", "latitude", "longitude")
        .transform(
            lambda d: dt.add_point_geometry(d, "latitude", "longitude", "geom_wgs84")
        )
        .transform(lambda d: dt.attach_h3_index(d, "geom_wgs84", H3_LEVEL, "h3_cell"))
    )


def cell_base(schools: DataFrame) -> DataFrame:
    """Contagem, total e média de matrículas por célula, com geometria da célula."""
    return schools.transform(
        lambda d: dt.aggregate_by_h3(
            d,
            "h3_cell",
            [
                F.count(F.lit(1)).cast("long").alias("n_schools"),
                F.sum("total_enrollment").cast("long").alias("total_enrollment"),
                F.avg("total_enrollment").alias("avg_enrollment"),
            ],
        )
    )


def neighbor_distances_by_cell(schools: DataFrame) -> DataFrame:
    """Média por célula da distância média de cada escola às suas vizinhas (km)."""
    return (
        schools.transform(
            lambda d: dt.h3_kring_neighbor_distances(
                d, "id_unidade", "h3_cell", "geom_wgs84", 1, "dist_neighbors_m"
            )
        )
        .groupBy("h3_cell")
        .agg(F.avg("dist_neighbors_m").alias("_dist_neighbors_m"))
    )


def nearest_distances_by_cell(schools: DataFrame) -> DataFrame:
    """Média por célula da distância de cada escola à federal mais próxima (km)."""
    nearest = schools.transform(
        lambda d: dt.nearest_neighbor_distance(d, "id_unidade", "geom_wgs84")
    )
    cells = schools.select("id_unidade", "h3_cell")
    return (
        nearest.join(cells, "id_unidade", "left")
        .groupBy("h3_cell")
        .agg(F.avg("dist_nearest_m").alias("_dist_nearest_m"))
    )


def main():
    """Grade H3 nível 5 das escolas federais em atividade: densidade, matrículas,
    vizinhança e distâncias médias (geodésicas) às vizinhas e à mais próxima."""
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = load_schools(spark).cache()

    base = cell_base(schools)
    pool = dt.count_h3_kring_members(schools, "h3_cell", 1, "_pool_size")
    neighbors = neighbor_distances_by_cell(schools)
    nearest = nearest_distances_by_cell(schools)

    grid = (
        base.join(pool, "h3_cell", "left")
        .join(neighbors, "h3_cell", "left")
        .join(nearest, "h3_cell", "left")
        .transform(dt.to_web_mercator)
        .select(
            "h3_cell",
            "n_schools",
            "total_enrollment",
            F.col("avg_enrollment").cast("double").alias("avg_enrollment"),
            (F.col("_pool_size") - F.lit(1)).cast("long").alias("n_federal_neighbors"),
            (F.col("_dist_neighbors_m") / F.lit(KM)).alias("avg_dist_neighbors_km"),
            (F.col("_dist_nearest_m") / F.lit(KM)).alias("avg_dist_nearest_km"),
            "geometry",
        )
    )

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-175415-r01/workdir/apps/educacao/jobs/silver/educacao_escola_federal_geo.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from apps.educacao.utils import column_transforms as educ_ct
from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_federal_geo", "silver")


def main():
    """Escolas federais em atividade: matrículas juntadas, ponto e geometria 3857.

    Guarda também latitude/longitude em WGS84 para que a camada gold possa medir
    distâncias geodésicas sobre as coordenadas originais.
    """
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-federal-geo-silver")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA)

    is_federal = educ_ct.is_federal(F.col("setor"))
    is_active = educ_ct.is_active(F.col("situacao"))
    enrollment = matricula.select(
        F.col("id_unidade"),
        F.col("mat_basica").alias("total_enrollment"),
    )

    geo = (
        escola.filter(is_federal & is_active)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .transform(lambda d: dt.join_no_fanout(d, enrollment, "id_unidade", "left"))
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "nome_unidade",
            "uf",
            "latitude",
            "longitude",
            F.col("total_enrollment").cast("int").alias("total_enrollment"),
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_federal_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

