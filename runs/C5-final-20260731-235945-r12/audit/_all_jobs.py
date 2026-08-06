# === /d/runs/C5-final-20260731-235945-r12/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# O raw aterrissa como CSV ';' em ISO-8859-1, particionado por ano, com uma
# subárvore de objetos por arquivo; ``recursiveFileLookup`` varre tudo.
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

# Colunas de indicador 0/1: vazio significa ausência de informação (NULL),
# nunca zero, então apenas tipamos.
INDICATOR_COLS = {
    "tem_agua": "TEM_AGUA",
    "tem_energia": "TEM_ENERGIA",
    "tem_esgoto": "TEM_ESGOTO",
    "tem_banheiro": "TEM_BANHEIRO",
    "tem_biblioteca": "TEM_BIBLIOTECA",
    "tem_lab_info": "TEM_LAB_INFO",
    "tem_quadra": "TEM_QUADRA",
    "tem_internet": "TEM_INTERNET",
}


def main():
    """Lê o raw das escolas, tipa as colunas em snake_case e salva a bronze."""
    spark = SparkSession.builder.appName("educacao-escola-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("encoding", "ISO-8859-1")
        .option("recursiveFileLookup", True)
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
        raw["SETOR"].cast("int").alias("setor"),
        raw["AREA"].cast("int").alias("area"),
        raw["SITUACAO"].cast("int").alias("situacao"),
        ct.to_coordinate(raw["LATITUDE"]).alias("latitude"),
        ct.to_coordinate(raw["LONGITUDE"]).alias("longitude"),
        raw["QT_SALAS"].cast("int").alias("qt_salas"),
        *[raw[src].cast("int").alias(dst) for dst, src in INDICATOR_COLS.items()],
    ).filter(ct.blank_to_null(raw["ID_UNIDADE"]).isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r12/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
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

# Contagens de matrícula: ausência de registro fica NULL, não zero.
COUNT_COLS = {
    "mat_basica": "MAT_BASICA",
    "mat_infantil": "MAT_INFANTIL",
    "mat_fundamental": "MAT_FUNDAMENTAL",
    "mat_medio": "MAT_MEDIO",
    "mat_profissional": "MAT_PROFISSIONAL",
    "mat_eja": "MAT_EJA",
    "mat_especial": "MAT_ESPECIAL",
}


def main():
    """Lê o raw das matrículas, tipa as contagens e salva a bronze."""
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("encoding", "ISO-8859-1")
        .option("recursiveFileLookup", True)
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(raw["ID_UNIDADE"], 8).alias("id_unidade"),
        *[raw[src].cast("int").alias(dst) for dst, src in COUNT_COLS.items()],
    ).filter(ct.blank_to_null(raw["ID_UNIDADE"]).isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r12/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5


def _to_km(geom_a: str, geom_b: str) -> "F.Column":
    """Distância geodésica (WGS84) entre dois pontos, em quilômetros."""
    return stf.ST_DistanceSpheroid(F.col(geom_a), F.col(geom_b)) / F.lit(1000.0)


def neighbor_stats(schools: DataFrame) -> DataFrame:
    """Por escola: nº de vizinhas e distância média (km) às vizinhas.

    Vizinhas são as outras escolas na própria célula H3 ou nas células
    imediatamente adjacentes (k-ring 1 = célula central + 6 anéis). Como o
    k-ring depende só da célula, todas as escolas de uma mesma célula têm o
    mesmo número de vizinhas.
    """
    left = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("geom_wgs84").alias("a_geom"),
        F.explode(
            stf.ST_H3KRing(F.col("h3_cell"), F.lit(1), F.lit(False))
        ).alias("ring_cell"),
    )
    right = schools.select(
        F.col("id_unidade").alias("b_id"),
        F.col("h3_cell").alias("b_cell"),
        F.col("geom_wgs84").alias("b_geom"),
    )
    pairs = (
        left.join(right, left["ring_cell"] == right["b_cell"], how="inner")
        .filter(F.col("a_id") != F.col("b_id"))
        .withColumn("dist_km", _to_km("a_geom", "b_geom"))
    )
    return pairs.groupBy(F.col("a_id").alias("id_unidade")).agg(
        F.count(F.lit(1)).cast("int").alias("n_neighbors"),
        F.avg("dist_km").alias("mean_dist_neighbors_km"),
    )


def nearest_stats(schools: DataFrame) -> DataFrame:
    """Por escola: distância (km) à federal em atividade mais próxima.

    Sem restrição de célula — considera todas as escolas federais em atividade,
    em qualquer lugar, exceto a própria.
    """
    a = schools.select(
        F.col("id_unidade").alias("a_id"), F.col("geom_wgs84").alias("a_geom")
    )
    b = schools.select(
        F.col("id_unidade").alias("b_id"), F.col("geom_wgs84").alias("b_geom")
    )
    pairs = (
        a.crossJoin(b)
        .filter(F.col("a_id") != F.col("b_id"))
        .withColumn("dist_km", _to_km("a_geom", "b_geom"))
    )
    return pairs.groupBy(F.col("a_id").alias("id_unidade")).agg(
        F.min("dist_km").alias("dist_nearest_km")
    )


def main():
    """Grade H3 nível 5 das escolas federais em atividade.

    Agrega por célula: contagem de escolas, matrículas totais e médias, número
    de vizinhas (constante na célula), distância média às vizinhas e distância
    média à federal mais próxima. Distâncias são geodésicas, medidas sobre as
    coordenadas WGS84 originais; a geometria da célula sai em EPSG:3857.
    """
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("geoparquet").load(ORIGEM)

    schools = (
        dt.add_point_geometry(silver, "latitude", "longitude", out_col="geom_wgs84")
        .transform(lambda d: dt.attach_h3_index(d, "geom_wgs84", H3_LEVEL, "h3_cell"))
        .select("id_unidade", "h3_cell", "enrollment", "geom_wgs84")
    ).cache()

    per_school = (
        schools.select("id_unidade", "h3_cell", "enrollment")
        .join(neighbor_stats(schools), on="id_unidade", how="left")
        .join(nearest_stats(schools), on="id_unidade", how="left")
        # sem vizinhas => 0 vizinhas (zero legítimo); a distância fica NULL.
        .withColumn("n_neighbors", F.coalesce(F.col("n_neighbors"), F.lit(0)))
    )

    grid = (
        per_school.groupBy("h3_cell")
        .agg(
            F.count(F.lit(1)).cast("long").alias("n_schools"),
            F.sum("enrollment").cast("long").alias("total_enrollment"),
            F.round(F.avg("enrollment"), 1).alias("avg_enrollment"),
            F.max("n_neighbors").cast("int").alias("n_federal_neighbors"),
            F.round(F.avg("mean_dist_neighbors_km"), 3).alias("avg_dist_neighbors_km"),
            F.round(F.avg("dist_nearest_km"), 3).alias("avg_dist_nearest_km"),
        )
        .withColumn(
            "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
        )
        .transform(dt.to_web_mercator)
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

# === /d/runs/C5-final-20260731-235945-r12/workdir/apps/educacao/jobs/silver/educacao_escola_matricula_geo.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula_geo", "silver")

# Setor 1 = federal; situação 1 = em atividade.
SETOR_FEDERAL = 1
SITUACAO_ATIVA = 1


def main():
    """Escolas federais em atividade, com matrículas e ponto georreferenciado.

    Filtra as federais em atividade, junta as matrículas (educação básica) pela
    unidade e materializa o ponto em EPSG:3857. Mantém latitude/longitude em
    WGS84 para que o gold meça distâncias geodésicas sobre as coordenadas
    originais, sem round-trip por projeção.
    """
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-geo-silver")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA)

    federais = (
        escola.filter(
            (F.col("setor") == SETOR_FEDERAL)
            & (F.col("situacao") == SITUACAO_ATIVA)
        )
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .select("id_unidade", "uf", "latitude", "longitude")
    )

    enrollment = matricula.select(
        "id_unidade", F.col("mat_basica").alias("enrollment")
    )

    geo = (
        dt.join_lookup(
            federais, enrollment, on="id_unidade", columns=["enrollment"], how="left"
        )
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select("id_unidade", "uf", "enrollment", "latitude", "longitude", "geometry")
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_matricula_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

