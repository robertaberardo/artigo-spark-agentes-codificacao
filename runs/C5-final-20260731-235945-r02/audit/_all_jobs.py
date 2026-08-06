# === /d/runs/C5-final-20260731-235945-r02/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# O raw aterrissa como CSV ";" com cabeçalho em MAIÚSCULAS (schema explícito,
# nunca inferSchema). Tudo entra como texto e é tipado na projeção da bronze.
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

# Indicadores 0/1 tipados como inteiro; vazio vira NULL no cast.
INDICADORES = [
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
    """Lê o raw das escolas, tipa/normaliza as colunas e grava a bronze."""
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
            raw[nome.upper()].cast("int").alias(nome)
            for nome in INDICADORES
        ],
    ).filter(ct.zero_pad_code(raw["ID_UNIDADE"], 8).isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r02/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
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

# Colunas de contagem de matrículas, tipadas como inteiro.
CONTAGENS = [
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
        *[raw[nome.upper()].cast("int").alias(nome) for nome in CONTAGENS],
    ).filter(ct.zero_pad_code(raw["ID_UNIDADE"], 8).isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r02/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
SETOR_FEDERAL = 1
SITUACAO_ATIVA = 1


def main():
    """Grade H3 (res 5) das escolas federais em atividade.

    Por célula: nº de escolas, total e média de matrículas, nº de escolas
    vizinhas (federais ativas na própria célula ou nas adjacentes — disco H3
    k=1 — exceto a própria escola; igual para toda a célula), distância média
    de cada escola às suas vizinhas e distância média de cada escola à federal
    ativa mais próxima (qualquer célula).

    Todas as distâncias são geodésicas (``ST_DistanceSpheroid``) medidas sobre o
    ponto WGS84 original; só a geometria da célula persiste em EPSG:3857.
    """
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("geoparquet").load(ORIGEM)

    # Federais em atividade, com ponto WGS84 original e célula H3 res 5.
    # (H3 opera em coordenadas geográficas; usamos o ponto 4326, não o 3857.)
    fed = (
        silver.filter(
            (F.col("setor") == SETOR_FEDERAL)
            & (F.col("situacao") == SITUACAO_ATIVA)
        )
        .withColumn(
            "point_wgs84", stc.ST_Point(F.col("longitude"), F.col("latitude"))
        )
        .withColumn(
            "h3_cell",
            F.element_at(stf.ST_H3CellIDs(F.col("point_wgs84"), H3_LEVEL, False), 1),
        )
        .select("id_unidade", "h3_cell", "total_enrollment", "point_wgs84")
    )
    # id_unidade é a chave; garante unicidade antes dos self-joins de distância.
    fed = dt.deduplicate_by_key(fed, ["id_unidade"]).cache()

    # --- Agregação básica por célula -----------------------------------------
    base = fed.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("total_enrollment").cast("long").alias("total_enrollment"),
        F.avg("total_enrollment").alias("avg_enrollment"),
    )

    # --- Vizinhas: federais ativas na própria célula ou nas adjacentes -------
    # Cada escola expande seu disco H3 k=1 (própria célula + 6 adjacentes) e
    # cruza com as demais escolas cuja célula caia nesse disco.
    source = fed.select(
        F.col("id_unidade").alias("sid"),
        F.col("h3_cell"),
        F.col("point_wgs84").alias("sgeom"),
    ).withColumn(
        "ring_cell",
        F.explode(stf.ST_H3KRing(F.col("h3_cell"), F.lit(1), F.lit(False))),
    )
    target = fed.select(
        F.col("id_unidade").alias("tid"),
        F.col("h3_cell").alias("tcell"),
        F.col("point_wgs84").alias("tgeom"),
    )
    pairs = (
        source.join(target, source["ring_cell"] == target["tcell"], "inner")
        .filter(F.col("sid") != F.col("tid"))
        .withColumn(
            "dist_km",
            stf.ST_DistanceSpheroid(F.col("sgeom"), F.col("tgeom")) / F.lit(1000.0),
        )
    )
    # Por escola: quantidade de vizinhas e distância média às vizinhas.
    per_school = pairs.groupBy("sid", "h3_cell").agg(
        F.count(F.lit(1)).alias("neighbor_count"),
        F.avg("dist_km").alias("mean_dist_neighbors_km"),
    )
    # Reincorpora escolas de células isoladas (sem vizinhas): count 0, dist NULL.
    per_school_full = fed.select(
        F.col("id_unidade").alias("sid"), F.col("h3_cell")
    ).join(per_school, ["sid", "h3_cell"], "left")

    neighbor_agg = per_school_full.groupBy("h3_cell").agg(
        # n_federal_neighbors = |N| - 1, idêntico para toda a célula.
        F.coalesce(F.max("neighbor_count"), F.lit(0)).cast("long").alias(
            "n_federal_neighbors"
        ),
        F.avg("mean_dist_neighbors_km").alias("avg_dist_neighbors_km"),
    )

    # --- Federal mais próxima (qualquer célula) ------------------------------
    left = fed.select(
        F.col("id_unidade").alias("aid"),
        F.col("h3_cell"),
        F.col("point_wgs84").alias("ageom"),
    )
    right = fed.select(
        F.col("id_unidade").alias("bid"),
        F.col("point_wgs84").alias("bgeom"),
    )
    nearest_per_school = (
        left.crossJoin(right)
        .filter(F.col("aid") != F.col("bid"))
        .withColumn(
            "dist_km",
            stf.ST_DistanceSpheroid(F.col("ageom"), F.col("bgeom")) / F.lit(1000.0),
        )
        .groupBy("aid", "h3_cell")
        .agg(F.min("dist_km").alias("nearest_km"))
    )
    nearest_agg = nearest_per_school.groupBy("h3_cell").agg(
        F.avg("nearest_km").alias("avg_dist_nearest_km")
    )

    # --- Consolidação por célula + geometria do hexágono (3857) --------------
    grid = (
        base.join(neighbor_agg, "h3_cell", "left")
        .join(nearest_agg, "h3_cell", "left")
        .withColumn(
            "geometry",
            stf.ST_Transform(
                F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1),
                F.lit("EPSG:4326"),
                F.lit("EPSG:3857"),
            ),
        )
        .select(
            "h3_cell",
            "n_schools",
            "total_enrollment",
            F.round("avg_enrollment", 2).alias("avg_enrollment"),
            "n_federal_neighbors",
            F.round("avg_dist_neighbors_km", 3).alias("avg_dist_neighbors_km"),
            F.round("avg_dist_nearest_km", 3).alias("avg_dist_nearest_km"),
            "geometry",
        )
    )

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r02/workdir/apps/educacao/jobs/silver/educacao_escola_matricula.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula", "silver")


def main():
    """Escolas georreferenciadas com matrículas: ponto WGS84 + geometria 3857.

    Mantém ``latitude``/``longitude`` originais para que a gold meça distâncias
    geodésicas sobre o ponto WGS84 (``ST_DistanceSpheroid``) sem round-trip de
    reprojeção; a geometria persistida vai para EPSG:3857, como no restante do
    lake.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA)

    # matrícula é única por id_unidade; join_no_fanout falha cedo se não for.
    total_enrollment = matricula.select(
        "id_unidade", F.col("mat_basica").alias("total_enrollment")
    )

    geo = (
        dt.join_no_fanout(escola, total_enrollment, on="id_unidade", how="left")
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "nome_unidade",
            "uf",
            "nome_municipio",
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
    print(f">> silver educacao_escola_matricula: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

