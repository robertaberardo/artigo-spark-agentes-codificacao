# === /d/runs/C5-final-20260731-235945-r10/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
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

# Colunas de infraestrutura (indicadores 0/1) tratadas em bloco.
FLAG_COLUMNS = [
    "tem_agua",
    "tem_energia",
    "tem_esgoto",
    "tem_banheiro",
    "tem_biblioteca",
    "tem_lab_info",
    "tem_quadra",
    "tem_internet",
]


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
        *[
            raw[src.upper()].cast("int").alias(src)
            for src in FLAG_COLUMNS
        ],
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r10/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
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

# Colunas de contagem de matrículas (todas inteiras não negativas).
COUNT_COLUMNS = [
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
        ct.zero_pad_code(raw["ID_UNIDADE"], 8).alias("id_unidade"),
        *[
            ct.null_if_negative(raw[src.upper()].cast("int")).alias(src)
            for src in COUNT_COLUMNS
        ],
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r10/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
FEDERAL = 1
EM_ATIVIDADE = 1
METERS_PER_KM = 1000.0


def federal_active_schools(silver: DataFrame) -> DataFrame:
    """Escolas federais em atividade, com ponto WGS84 e célula H3 nível 5.

    O ``point`` é reconstruído em WGS84 (lon/lat) a partir das coordenadas
    originais: as distâncias geodésicas (``ST_DistanceSpheroid``) exigem 4326 e não
    se reprojeta o ponto 3857 de volta só para medir. A célula H3 sai desse ponto.
    """
    point = stc.ST_Point(F.col("longitude"), F.col("latitude"))
    cell = F.element_at(stf.ST_H3CellIDs(F.col("point"), H3_LEVEL, False), 1)
    return (
        silver.filter(
            (F.col("setor") == FEDERAL) & (F.col("situacao") == EM_ATIVIDADE)
        )
        .withColumn("point", point)
        .withColumn("h3_cell", cell)
        .select("id_unidade", "total_enrollment", "h3_cell", "point")
    )


def cell_aggregates(schools: DataFrame) -> DataFrame:
    """Contagem e matrículas por célula: ``n_schools``/``total_enrollment``/``avg``."""
    return schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("total_enrollment").cast("long").alias("total_enrollment"),
        F.round(F.avg("total_enrollment"), 2).alias("avg_enrollment"),
    )


def neighbor_counts(schools: DataFrame) -> DataFrame:
    """``n_federal_neighbors`` por célula = (escolas no disco kRing(1)) − 1.

    Cada escola é "coberta" pelas células do seu disco kRing(1) (própria + 6
    adjacentes). Como uma escola na célula X é vizinha do alvo C sse X está no
    disco de C (relação simétrica), basta explodir cada escola sobre o disco e
    contar por célula-alvo. Subtrai 1 para excluir a própria escola — o resultado
    é o número de vizinhas de qualquer escola da célula (igual para toda a célula).
    """
    ring = stf.ST_H3KRing(F.col("h3_cell"), 1, False)
    membership = schools.select("id_unidade", F.explode(ring).alias("target_cell"))
    return membership.groupBy("target_cell").agg(
        (F.count(F.lit(1)) - F.lit(1)).cast("long").alias("n_federal_neighbors")
    )


def neighbor_distance(schools: DataFrame) -> DataFrame:
    """``avg_dist_neighbors_km`` por célula (média das médias por escola).

    Para cada escola calcula a distância geodésica às vizinhas (escolas cujo
    disco kRing(1) contém a célula da escola) e tira a média; depois promedia
    essas médias na célula. Escolas isoladas não geram pares — a célula fica
    ``NULL`` quando nenhuma escola tem vizinha (ausência não é zero).
    """
    ring = stf.ST_H3KRing(F.col("h3_cell"), 1, False)
    origin = schools.select(
        F.col("id_unidade").alias("s_id"),
        F.col("h3_cell").alias("s_cell"),
        F.col("point").alias("s_point"),
        F.explode(ring).alias("ring_cell"),
    )
    target = schools.select(
        F.col("id_unidade").alias("t_id"),
        F.col("h3_cell").alias("t_cell"),
        F.col("point").alias("t_point"),
    )
    pairs = origin.join(
        target, F.col("ring_cell") == F.col("t_cell"), "inner"
    ).filter(F.col("s_id") != F.col("t_id"))

    per_school = pairs.withColumn(
        "dist_m", stf.ST_DistanceSpheroid(F.col("s_point"), F.col("t_point"))
    ).groupBy("s_cell", "s_id").agg(F.avg("dist_m").alias("school_avg_dist_m"))

    return (
        per_school.groupBy("s_cell")
        .agg(F.round(F.avg("school_avg_dist_m") / F.lit(METERS_PER_KM), 3).alias("avg_dist_neighbors_km"))
        .withColumnRenamed("s_cell", "h3_cell")
    )


def nearest_distance(schools: DataFrame) -> DataFrame:
    """``avg_dist_nearest_km`` por célula.

    Distância geodésica de cada escola à federal mais próxima (qualquer, sem
    restrição de célula), via produto cartesiano das federais em atividade, depois
    promediada por célula. O volume é pequeno (centenas de escolas), então o cross
    join é viável.
    """
    origin = schools.select(
        F.col("id_unidade").alias("s_id"),
        F.col("h3_cell").alias("s_cell"),
        F.col("point").alias("s_point"),
    )
    target = schools.select(
        F.col("id_unidade").alias("t_id"),
        F.col("point").alias("t_point"),
    )
    pairs = origin.crossJoin(target).filter(F.col("s_id") != F.col("t_id"))

    per_school = pairs.withColumn(
        "dist_m", stf.ST_DistanceSpheroid(F.col("s_point"), F.col("t_point"))
    ).groupBy("s_cell", "s_id").agg(F.min("dist_m").alias("nearest_m"))

    return (
        per_school.groupBy("s_cell")
        .agg(F.round(F.avg("nearest_m") / F.lit(METERS_PER_KM), 3).alias("avg_dist_nearest_km"))
        .withColumnRenamed("s_cell", "h3_cell")
    )


def main():
    """Grade H3 nível 5 das escolas federais em atividade (densidade + distâncias)."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("geoparquet").load(ORIGEM)
    schools = federal_active_schools(silver).cache()

    cells = cell_aggregates(schools)
    neighbors = neighbor_counts(schools)
    neigh_dist = neighbor_distance(schools)
    near_dist = nearest_distance(schools)

    cell_geom = F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
    grid = (
        cells.join(neighbors, cells["h3_cell"] == neighbors["target_cell"], "left")
        .drop("target_cell")
        .join(neigh_dist, "h3_cell", "left")
        .join(near_dist, "h3_cell", "left")
        .withColumn("geometry", cell_geom)
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

# === /d/runs/C5-final-20260731-235945-r10/workdir/apps/educacao/jobs/silver/educacao_escola_matricula.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula", "silver")


def main():
    """Escolas georreferenciadas com as matrículas anexadas (ponto em 3857).

    Junta a bronze de escolas com a de matrículas pela chave ``id_unidade``,
    mantém apenas as escolas com coordenada válida, constrói o ponto WGS84 e o
    persiste em EPSG:3857. Guarda ``latitude``/``longitude`` para as medidas
    geodésicas do gold, além de ``setor``/``situacao`` para os recortes.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA).select(
        F.col("id_unidade"),
        F.col("mat_basica").alias("total_enrollment"),
    )

    silver = (
        dt.join_no_fanout(escola, matricula, on="id_unidade", how="left")
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "nome_unidade",
            "uf",
            "nome_municipio",
            "cod_municipio",
            "setor",
            "situacao",
            "latitude",
            "longitude",
            "total_enrollment",
            "geometry",
        )
    )

    silver.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_matricula: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

