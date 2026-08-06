# === /d/runs/C5-final-20260801-143919-r01/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

RAW_SCHEMA = T.StructType(
    [
        T.StructField("ID_UNIDADE", T.StringType()),
        T.StructField("NOME_UNIDADE", T.StringType()),
        T.StructField("UF", T.StringType()),
        T.StructField("COD_UF", T.StringType()),
        T.StructField("NOME_MUNICIPIO", T.StringType()),
        T.StructField("COD_MUNICIPIO", T.StringType()),
        T.StructField("SETOR", T.StringType()),
        T.StructField("AREA", T.StringType()),
        T.StructField("SITUACAO", T.StringType()),
        T.StructField("LATITUDE", T.StringType()),
        T.StructField("LONGITUDE", T.StringType()),
        T.StructField("QT_SALAS", T.StringType()),
        T.StructField("TEM_AGUA", T.StringType()),
        T.StructField("TEM_ENERGIA", T.StringType()),
        T.StructField("TEM_ESGOTO", T.StringType()),
        T.StructField("TEM_BANHEIRO", T.StringType()),
        T.StructField("TEM_BIBLIOTECA", T.StringType()),
        T.StructField("TEM_LAB_INFO", T.StringType()),
        T.StructField("TEM_QUADRA", T.StringType()),
        T.StructField("TEM_INTERNET", T.StringType()),
    ]
)


def main():
    """Lê o raw das escolas do Censo, tipa os campos e grava a bronze.

    Códigos administrativos (setor/área/situação) permanecem como texto — são
    categorias, não quantidades. Lat/long viram ``double`` para o geoespacial;
    a chave é normalizada para 8 dígitos com zero à esquerda, casando com o
    formato das matrículas.
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
        ct.blank_to_null(F.col("QT_SALAS")).cast(T.IntegerType()).alias("qt_salas"),
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-143919-r01/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

RAW_SCHEMA = T.StructType(
    [
        T.StructField("ID_UNIDADE", T.StringType()),
        T.StructField("MAT_BASICA", T.StringType()),
        T.StructField("MAT_INFANTIL", T.StringType()),
        T.StructField("MAT_FUNDAMENTAL", T.StringType()),
        T.StructField("MAT_MEDIO", T.StringType()),
        T.StructField("MAT_PROFISSIONAL", T.StringType()),
        T.StructField("MAT_EJA", T.StringType()),
        T.StructField("MAT_ESPECIAL", T.StringType()),
    ]
)

# As contagens de matrícula são inteiros; a caixa vira snake_case já no bronze.
MAT_COLUMNS = {
    "mat_basica": "MAT_BASICA",
    "mat_infantil": "MAT_INFANTIL",
    "mat_fundamental": "MAT_FUNDAMENTAL",
    "mat_medio": "MAT_MEDIO",
    "mat_profissional": "MAT_PROFISSIONAL",
    "mat_eja": "MAT_EJA",
    "mat_especial": "MAT_ESPECIAL",
}


def main():
    """Lê o raw das matrículas, converte as contagens em inteiros e grava a bronze."""
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
        *[
            ct.blank_to_null(F.col(src)).cast(T.IntegerType()).alias(dst)
            for dst, src in MAT_COLUMNS.items()
        ],
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-143919-r01/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
# k=1 (com exact_ring=False) devolve a célula e as adjacentes imediatas.
KRING_K = 1
KRING_EXACT = False
METERS_PER_KM = 1000.0


def geodesic_km(point_a, point_b):
    """Distância geodésica (km) entre dois pontos WGS84, via ``ST_DistanceSpheroid``.

    Mede sobre o elipsoide WGS84, nas coordenadas originais em graus — nunca sobre
    a projeção métrica, que distorce os metros nas latitudes do Brasil.
    """
    return stf.ST_DistanceSpheroid(point_a, point_b) / F.lit(METERS_PER_KM)


def prepare_base(geo: DataFrame) -> DataFrame:
    """Reconstrói o ponto WGS84 original e anexa a célula H3 nível 5.

    O ponto é remontado de lat/long (não da geometria 3857 persistida) para que
    as distâncias sejam medidas nas coordenadas originais. A célula H3 é calculada
    em EPSG:4326, como o H3 exige.
    """
    return (
        geo.select("id_unidade", "total_enrollment", "latitude", "longitude")
        .withColumn("point", stc.ST_Point(F.col("longitude"), F.col("latitude")))
        .withColumn(
            "h3_cell", F.element_at(stf.ST_H3CellIDs(F.col("point"), H3_LEVEL, False), 1)
        )
    )


def cell_enrollment_stats(base: DataFrame) -> DataFrame:
    """Contagem de escolas, total e média de matrículas por célula."""
    return base.groupBy("h3_cell").agg(
        F.count(F.lit(1)).alias("n_schools"),
        F.sum("total_enrollment").alias("total_enrollment"),
        F.round(F.avg("total_enrollment"), 1).alias("avg_enrollment"),
    )


def neighbor_distances(base: DataFrame) -> DataFrame:
    """Vizinhança por célula: quantidade de vizinhas e distância média a elas.

    São vizinhas as outras escolas federais em atividade na própria célula ou nas
    adjacentes (disco H3 de raio 1). Explode-se o disco de cada escola em células
    candidatas e casa-se com as escolas que caem nelas; o par com a própria escola
    é descartado. A contagem de vizinhas é idêntica para toda a célula (todas as
    escolas da célula compartilham o mesmo disco), então basta ``max`` por célula.
    """
    origin = base.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("point").alias("a_point"),
    ).withColumn(
        "ring_cell", F.explode(stf.ST_H3KRing(F.col("a_cell"), KRING_K, KRING_EXACT))
    )
    candidate = base.select(
        F.col("id_unidade").alias("b_id"),
        F.col("h3_cell").alias("b_cell"),
        F.col("point").alias("b_point"),
    )

    pairs = origin.join(
        candidate, origin["ring_cell"] == candidate["b_cell"], "inner"
    ).filter(F.col("a_id") != F.col("b_id"))

    per_school = pairs.groupBy("a_id", "a_cell").agg(
        F.count(F.lit(1)).alias("n_neighbors"),
        F.avg(geodesic_km(F.col("a_point"), F.col("b_point"))).alias("dist_neighbors_km"),
    )

    return per_school.groupBy("a_cell").agg(
        F.max("n_neighbors").alias("n_federal_neighbors"),
        F.round(F.avg("dist_neighbors_km"), 3).alias("avg_dist_neighbors_km"),
    ).select(
        F.col("a_cell").alias("h3_cell"),
        "n_federal_neighbors",
        "avg_dist_neighbors_km",
    )


def nearest_federal_distances(base: DataFrame) -> DataFrame:
    """Distância média, por célula, de cada escola à federal mais próxima.

    Sem restrição de célula: cruza todas as escolas federais em atividade entre si
    (o universo já é só federais ativas), toma a menor distância de cada uma e
    tira a média por célula. O conjunto é pequeno (centenas), então o produto
    cartesiano é barato.
    """
    origin = base.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("point").alias("a_point"),
    )
    other = base.select(
        F.col("id_unidade").alias("o_id"),
        F.col("point").alias("o_point"),
    )

    nearest = (
        origin.crossJoin(other)
        .filter(F.col("a_id") != F.col("o_id"))
        .groupBy("a_id", "a_cell")
        .agg(F.min(geodesic_km(F.col("a_point"), F.col("o_point"))).alias("dist_nearest_km"))
    )

    return nearest.groupBy("a_cell").agg(
        F.round(F.avg("dist_nearest_km"), 3).alias("avg_dist_nearest_km")
    ).select(F.col("a_cell").alias("h3_cell"), "avg_dist_nearest_km")


def add_cell_geometry(df: DataFrame) -> DataFrame:
    """Anexa o polígono da célula H3 em EPSG:3857 (convenção de saída do lake)."""
    cell_geom = F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
    return df.withColumn(
        "geometry", stf.ST_Transform(cell_geom, F.lit("EPSG:4326"), F.lit("EPSG:3857"))
    )


def main():
    """Grade H3 nível 5 das escolas federais em atividade, com matrículas e vizinhança."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    geo = spark.read.format("geoparquet").load(ORIGEM)
    base = prepare_base(geo).cache()

    stats = cell_enrollment_stats(base)
    neighbors = neighbor_distances(base)
    nearest = nearest_federal_distances(base)

    grid = (
        stats.join(neighbors, on="h3_cell", how="left")
        .join(nearest, on="h3_cell", how="left")
        # Célula sem vizinhas (escola isolada na própria célula e adjacentes): a
        # contagem é 0 de fato; a distância média fica NULL (indefinida).
        .withColumn("n_federal_neighbors", F.coalesce(F.col("n_federal_neighbors"), F.lit(0)))
        .transform(add_cell_geometry)
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

# === /d/runs/C5-final-20260801-143919-r01/workdir/apps/educacao/jobs/silver/educacao_escola_geo.py ===
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_geo", "silver")

# Códigos do Censo: setor 1 = federal; situação 1 = em atividade.
SETOR_FEDERAL = "1"
SITUACAO_ATIVA = "1"


def keep_federal_active(df: DataFrame) -> DataFrame:
    """Mantém apenas as escolas do setor federal em atividade."""
    is_federal = F.col("setor") == SETOR_FEDERAL
    is_active = F.col("situacao") == SITUACAO_ATIVA
    return df.filter(is_federal & is_active)


def attach_enrollment(escola: DataFrame, matricula: DataFrame) -> DataFrame:
    """Enriquece as escolas com o total de matrículas (mat_basica) da unidade.

    ``left join`` para preservar toda escola federal em atividade mesmo sem
    registro de matrícula; a ausência fica ``NULL`` (não zero). ``join_no_fanout``
    garante que a matrícula é única por unidade e não multiplica linhas.
    """
    enrollment = matricula.select(
        "id_unidade", F.col("mat_basica").alias("total_enrollment")
    )
    return dt.join_no_fanout(escola, enrollment, on="id_unidade", how="left")


def main():
    """Escolas federais em atividade, georreferenciadas e com total de matrículas.

    Guarda lat/long WGS84 originais (para medir distâncias geodésicas no gold,
    antes de qualquer projeção) e a geometria do ponto em EPSG:3857, convenção
    de saída do lake.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA)

    geo = (
        escola.transform(keep_federal_active)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .transform(lambda d: attach_enrollment(d, matricula))
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "uf",
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

