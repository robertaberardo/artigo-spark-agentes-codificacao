# === /d/runs/C5-final-20260731-235945-r05/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
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


def main():
    """Lê o raw das escolas, padroniza nomes/tipos e salva a bronze.

    Nomes de coluna viram snake_case; lat/long viram ``double`` (nunca string) e
    os indicadores 0/1 viram ``int``. A ausência é preservada como ``NULL``.
    """
    spark = SparkSession.builder.appName("educacao-escola-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(F.col("ID_UNIDADE"), 8).alias("id_unidade"),
        ct.normalize_text(F.col("NOME_UNIDADE")).alias("nome_unidade"),
        ct.standardize_uf(F.col("UF")).alias("uf"),
        ct.blank_to_null(F.col("COD_UF")).alias("cod_uf"),
        ct.normalize_text(F.col("NOME_MUNICIPIO")).alias("nome_municipio"),
        ct.blank_to_null(F.col("COD_MUNICIPIO")).alias("cod_municipio"),
        ct.blank_to_null(F.col("SETOR")).alias("setor"),
        ct.blank_to_null(F.col("AREA")).alias("area"),
        ct.blank_to_null(F.col("SITUACAO")).alias("situacao"),
        ct.to_coordinate(F.col("LATITUDE")).alias("latitude"),
        ct.to_coordinate(F.col("LONGITUDE")).alias("longitude"),
        ct.blank_to_null(F.col("QT_SALAS")).cast("int").alias("qt_salas"),
        ct.blank_to_null(F.col("TEM_AGUA")).cast("int").alias("tem_agua"),
        ct.blank_to_null(F.col("TEM_ENERGIA")).cast("int").alias("tem_energia"),
        ct.blank_to_null(F.col("TEM_ESGOTO")).cast("int").alias("tem_esgoto"),
        ct.blank_to_null(F.col("TEM_BANHEIRO")).cast("int").alias("tem_banheiro"),
        ct.blank_to_null(F.col("TEM_BIBLIOTECA")).cast("int").alias("tem_biblioteca"),
        ct.blank_to_null(F.col("TEM_LAB_INFO")).cast("int").alias("tem_lab_info"),
        ct.blank_to_null(F.col("TEM_QUADRA")).cast("int").alias("tem_quadra"),
        ct.blank_to_null(F.col("TEM_INTERNET")).cast("int").alias("tem_internet"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r05/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
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


def main():
    """Lê o raw das matrículas, padroniza nomes/tipos e salva a bronze.

    Nomes viram snake_case e as contagens viram ``int``; a ausência é preservada
    como ``NULL`` (nunca zero).
    """
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(F.col("ID_UNIDADE"), 8).alias("id_unidade"),
        ct.blank_to_null(F.col("MAT_BASICA")).cast("int").alias("mat_basica"),
        ct.blank_to_null(F.col("MAT_INFANTIL")).cast("int").alias("mat_infantil"),
        ct.blank_to_null(F.col("MAT_FUNDAMENTAL")).cast("int").alias("mat_fundamental"),
        ct.blank_to_null(F.col("MAT_MEDIO")).cast("int").alias("mat_medio"),
        ct.blank_to_null(F.col("MAT_PROFISSIONAL")).cast("int").alias("mat_profissional"),
        ct.blank_to_null(F.col("MAT_EJA")).cast("int").alias("mat_eja"),
        ct.blank_to_null(F.col("MAT_ESPECIAL")).cast("int").alias("mat_especial"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r05/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

# Anel de vizinhança: a própria célula + as imediatamente adjacentes (k=1).
NEIGHBOR_K = 1


def load_schools(spark) -> DataFrame:
    """Escolas federais em atividade com o ponto WGS84 reconstruído do lat/long.

    A distância geodésica precisa das coordenadas WGS84 originais; por isso o
    ponto é montado a partir de lat/long (``double``), sem reusar a geometria
    persistida em 3857 (evita o round-trip 3857→4326).
    """
    geo = spark.read.format("geoparquet").load(ORIGEM)
    return geo.select(
        "id_unidade",
        "h3_cell",
        "total_enrollment",
        stc.ST_Point(F.col("longitude"), F.col("latitude")).alias("wgs"),
    )


def neighbor_metrics(schools: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Contagem de vizinhas por célula e distância média às vizinhas por célula.

    Vizinhas são as outras escolas na própria célula ou nas adjacentes (anel H3
    k=1). O total de escolas do anel é igual para toda a célula, então cada
    escola tem ``pool - 1`` vizinhas (exclui a si mesma) — o valor de
    ``n_federal_neighbors``. As distâncias usam só os pares de escolas distintas.
    """
    left = schools.select(
        F.col("id_unidade").alias("sid"),
        F.col("h3_cell").alias("lcell"),
        F.col("wgs").alias("lgeom"),
    ).withColumn("ring_cell", F.explode(stf.ST_H3KRing(F.col("lcell"), NEIGHBOR_K, False)))
    right = schools.select(
        F.col("id_unidade").alias("rid"),
        F.col("h3_cell").alias("rcell"),
        F.col("wgs").alias("rgeom"),
    )
    pairs = left.join(right, F.col("ring_cell") == F.col("rcell"), "inner")

    pool = pairs.groupBy("lcell").agg(
        (F.count_distinct("rid") - F.lit(1)).cast("long").alias("n_federal_neighbors")
    ).withColumnRenamed("lcell", "h3_cell")

    # Distância geodésica (m) entre pares de escolas distintas, no WGS84 original.
    neighbors = pairs.filter(F.col("sid") != F.col("rid")).withColumn(
        "dist_m", stf.ST_DistanceSpheroid(F.col("lgeom"), F.col("rgeom"))
    )
    per_school = neighbors.groupBy("sid", "lcell").agg(
        F.avg("dist_m").alias("school_avg_dist_m")
    )
    per_cell = per_school.groupBy("lcell").agg(
        F.avg("school_avg_dist_m").alias("avg_dist_neighbors_m")
    ).withColumnRenamed("lcell", "h3_cell")
    return pool, per_cell


def nearest_metrics(schools: DataFrame) -> DataFrame:
    """Distância média, por célula, de cada escola à federal mais próxima.

    Sem restrição de célula: para cada escola busca a menor distância geodésica
    a qualquer outra federal em atividade, depois faz a média por célula.
    """
    a = schools.select(
        F.col("id_unidade").alias("aid"),
        F.col("h3_cell").alias("acell"),
        F.col("wgs").alias("ageom"),
    )
    b = schools.select(
        F.col("id_unidade").alias("bid"),
        F.col("wgs").alias("bgeom"),
    )
    pairs = a.crossJoin(b).filter(F.col("aid") != F.col("bid")).withColumn(
        "dist_m", stf.ST_DistanceSpheroid(F.col("ageom"), F.col("bgeom"))
    )
    per_school = pairs.groupBy("aid", "acell").agg(F.min("dist_m").alias("nearest_m"))
    return per_school.groupBy("acell").agg(
        F.avg("nearest_m").alias("avg_dist_nearest_m")
    ).withColumnRenamed("acell", "h3_cell")


def main():
    """Grade H3 nível 5 das escolas federais em atividade (contagem + distâncias)."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = load_schools(spark).cache()

    base = schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("total_enrollment").cast("long").alias("total_enrollment"),
        F.avg("total_enrollment").alias("avg_enrollment"),
    )
    pool, neighbors = neighbor_metrics(schools)
    nearest = nearest_metrics(schools)

    grid = (
        base.transform(lambda d: dt.join_no_fanout(d, pool, "h3_cell", "left"))
        .transform(lambda d: dt.join_no_fanout(d, neighbors, "h3_cell", "left"))
        .transform(lambda d: dt.join_no_fanout(d, nearest, "h3_cell", "left"))
        .withColumn(
            "geometry",
            stf.ST_Transform(
                F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1),
                F.lit("EPSG:4326"),
                F.lit("EPSG:3857"),
            ),
        )
    )

    result = grid.select(
        F.col("h3_cell"),
        F.col("n_schools"),
        F.col("total_enrollment"),
        F.round(F.col("avg_enrollment"), 2).alias("avg_enrollment"),
        F.coalesce(F.col("n_federal_neighbors"), F.lit(0)).alias("n_federal_neighbors"),
        F.round(F.col("avg_dist_neighbors_m") / F.lit(1000.0), 3).alias("avg_dist_neighbors_km"),
        F.round(F.col("avg_dist_nearest_m") / F.lit(1000.0), 3).alias("avg_dist_nearest_km"),
        F.col("geometry"),
    )

    result.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r05/workdir/apps/educacao/jobs/silver/educacao_escola_matricula_geo.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula_geo", "silver")

# Filtros de recorte: setor 1 = federal, situação 1 = em atividade.
SETOR_FEDERAL = "1"
SITUACAO_ATIVIDADE = "1"

# Nível da grade H3 pedido para a agregação a jusante.
H3_LEVEL = 5


def main():
    """Escolas federais em atividade, georreferenciadas e com matrículas.

    Recorta o cadastro (federal + em atividade), constrói o ponto WGS84, indexa
    a célula H3 nível 5 sobre o ponto original e enriquece com o total de
    matrículas. Preserva lat/long em ``double`` para a distância geodésica no
    gold e persiste a geometria em EPSG:3857.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA).filter(
        (F.col("setor") == SETOR_FEDERAL) & (F.col("situacao") == SITUACAO_ATIVIDADE)
    )
    matricula = spark.read.parquet(MATRICULA).select(
        F.col("id_unidade"),
        F.col("mat_basica").alias("total_enrollment"),
    )

    geo = (
        escola.transform(
            lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude")
        )
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        # H3 é calculado sobre o ponto WGS84 ORIGINAL, antes de projetar.
        .transform(lambda d: dt.attach_h3_index(d, "geometry", H3_LEVEL, "h3_cell"))
        .transform(dt.to_web_mercator)
        .transform(
            lambda d: dt.join_no_fanout(d, matricula, "id_unidade", "left")
        )
        .select(
            "id_unidade",
            "uf",
            "latitude",
            "longitude",
            "total_enrollment",
            "h3_cell",
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_matricula_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

