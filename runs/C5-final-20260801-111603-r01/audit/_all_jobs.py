# === /d/runs/C5-final-20260801-111603-r01/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.educacao.utils import column_transforms as edu_ct
from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# O raw traz uma coluna extra `ano` ao final; como o schema é aplicado por
# posição, declarar só as 20 colunas de interesse já a descarta.
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

INDICADORES = [
    "TEM_AGUA",
    "TEM_ENERGIA",
    "TEM_ESGOTO",
    "TEM_BANHEIRO",
    "TEM_BIBLIOTECA",
    "TEM_LAB_INFO",
    "TEM_QUADRA",
    "TEM_INTERNET",
]


def main():
    """Lê o raw das escolas, tipa as colunas e salva a bronze."""
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
        raw["SETOR"].cast("int").alias("setor"),
        raw["AREA"].cast("int").alias("area"),
        raw["SITUACAO"].cast("int").alias("situacao"),
        ct.to_coordinate(raw["LATITUDE"]).alias("latitude"),
        ct.to_coordinate(raw["LONGITUDE"]).alias("longitude"),
        raw["QT_SALAS"].cast("int").alias("qt_salas"),
        *[raw[c].cast("int").alias(c.lower()) for c in INDICADORES],
    ).filter(edu_ct.is_valid_unidade_code(raw["ID_UNIDADE"]))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-111603-r01/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from apps.educacao.utils import column_transforms as edu_ct
from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

# O raw traz uma coluna extra `ano` ao final; o schema por posição a descarta.
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

MATRICULAS = [
    "MAT_BASICA",
    "MAT_INFANTIL",
    "MAT_FUNDAMENTAL",
    "MAT_MEDIO",
    "MAT_PROFISSIONAL",
    "MAT_EJA",
    "MAT_ESPECIAL",
]


def main():
    """Lê o raw das matrículas, tipa as colunas e salva a bronze."""
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
        *[raw[c].cast("int").alias(c.lower()) for c in MATRICULAS],
    ).filter(edu_ct.is_valid_unidade_code(raw["ID_UNIDADE"]))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-111603-r01/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
KRING_ADJACENTES = 1
METERS_PER_KM = 1000.0


def index_schools(df: DataFrame) -> DataFrame:
    """Anexa o ponto WGS84 e a célula H3 nível 5 de cada escola.

    O ponto é reconstruído das coordenadas originais (não da geometria 3857 do
    silver): tanto o H3 quanto ``ST_DistanceSpheroid`` operam sobre WGS84, e usar
    a geometria métrica atribuiria células erradas e mediria distâncias tortas.
    """
    with_point = df.withColumn(
        "point", stc.ST_Point(F.col("longitude"), F.col("latitude"))
    )
    return with_point.withColumn(
        "h3_cell", F.element_at(stf.ST_H3CellIDs(F.col("point"), H3_LEVEL, False), 1)
    )


def cell_basic_stats(schools: DataFrame) -> DataFrame:
    """Contagem de escolas, total e média de matrículas por célula."""
    return schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("enrollment").cast("long").alias("total_enrollment"),
        F.round(F.avg("enrollment"), 2).alias("avg_enrollment"),
    )


def cell_nearest_stats(schools: DataFrame) -> DataFrame:
    """Distância média à federal mais próxima (qualquer, sem restrição de célula).

    Para cada escola mede a distância geodésica a todas as demais e fica com a
    menor; depois tira a média por célula.
    """
    left = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell"),
        F.col("point").alias("a_point"),
    )
    right = schools.select(
        F.col("id_unidade").alias("b_id"), F.col("point").alias("b_point")
    )
    per_school = (
        left.crossJoin(right)
        .filter(F.col("a_id") != F.col("b_id"))
        .withColumn("dist_m", stf.ST_DistanceSpheroid(F.col("a_point"), F.col("b_point")))
        .groupBy("a_id", "h3_cell")
        .agg(F.min("dist_m").alias("nearest_m"))
    )
    return per_school.groupBy("h3_cell").agg(
        F.round(F.avg("nearest_m") / METERS_PER_KM, 3).alias("avg_dist_nearest_km")
    )


def _disk_schools(schools: DataFrame) -> DataFrame:
    """Escolas do disco (célula + adjacentes) de cada célula focal.

    Expande cada célula focal em seu k-ring 1 (a própria célula e as seis
    vizinhas) e casa com as escolas situadas nessas células.
    """
    cell_disk = (
        schools.select("h3_cell")
        .distinct()
        .withColumn(
            "disk_cell",
            F.explode(stf.ST_H3KRing(F.col("h3_cell"), KRING_ADJACENTES, False)),
        )
    )
    neighbors = schools.select(
        F.col("h3_cell").alias("disk_cell"),
        F.col("id_unidade").alias("nb_id"),
        F.col("point").alias("nb_point"),
    )
    return cell_disk.join(neighbors, on="disk_cell", how="inner")


def cell_neighbor_stats(schools: DataFrame) -> DataFrame:
    """Quantidade de vizinhas e distância média de cada escola às suas vizinhas.

    Vizinhas de uma escola são as demais federais em atividade da própria célula
    ou das adjacentes. O disco é idêntico para todas as escolas da célula, então
    a contagem (escolas do disco menos a própria) é a mesma para toda a célula.
    """
    disk_schools = _disk_schools(schools)

    counts = disk_schools.groupBy("h3_cell").agg(
        (F.countDistinct("nb_id") - F.lit(1)).cast("long").alias("n_federal_neighbors")
    )

    focal = schools.select(
        "h3_cell",
        F.col("id_unidade").alias("f_id"),
        F.col("point").alias("f_point"),
    )
    distances = (
        focal.join(
            disk_schools.select("h3_cell", "nb_id", "nb_point"), on="h3_cell", how="inner"
        )
        .filter(F.col("f_id") != F.col("nb_id"))
        .withColumn("dist_m", stf.ST_DistanceSpheroid(F.col("f_point"), F.col("nb_point")))
        .groupBy("h3_cell")
        .agg(F.round(F.avg("dist_m") / METERS_PER_KM, 3).alias("avg_dist_neighbors_km"))
    )
    return counts.join(distances, on="h3_cell", how="left")


def main():
    """Grade H3 nível 5 de escolas federais em atividade, com vizinhança."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = index_schools(spark.read.format("geoparquet").load(ORIGEM)).cache()

    grid = (
        cell_basic_stats(schools)
        .join(cell_neighbor_stats(schools), on="h3_cell", how="left")
        .join(cell_nearest_stats(schools), on="h3_cell", how="left")
        .withColumn(
            "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
        )
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

# === /d/runs/C5-final-20260801-111603-r01/workdir/apps/educacao/jobs/silver/educacao_escola_geo.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_geo", "silver")

SETOR_FEDERAL = 1
SITUACAO_EM_ATIVIDADE = 1


def filter_federal_ativa(df):
    """Mantém apenas escolas federais (setor 1) em atividade (situação 1)."""
    is_federal = F.col("setor") == SETOR_FEDERAL
    is_ativa = F.col("situacao") == SITUACAO_EM_ATIVIDADE
    return df.filter(is_federal & is_ativa)


def main():
    """Escolas federais em atividade, georreferenciadas e com matrículas.

    Preserva latitude/longitude em WGS84 além da geometria persistida em 3857:
    a grade H3 e as distâncias geodésicas do gold são calculadas sobre o CRS
    geográfico original, sem o round-trip 4326→3857→4326 (que perderia precisão)
    nem indexar H3 sobre coordenadas métricas (que produziria células erradas).
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA).select("id_unidade", "mat_basica")

    geo = (
        escola.transform(filter_federal_ativa)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(lambda d: dt.join_lookup(d, matricula, "id_unidade", ["mat_basica"]))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "uf",
            F.col("mat_basica").alias("enrollment"),
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

