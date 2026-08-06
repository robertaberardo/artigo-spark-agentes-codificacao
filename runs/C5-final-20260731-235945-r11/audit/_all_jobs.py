# === /d/runs/C5-final-20260731-235945-r11/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# Indicadores de infraestrutura (0/1). Ficam num grupo à parte porque recebem o
# mesmo tratamento (yes_no_to_int) e viram int.
INDICATOR_COLS = [
    "TEM_AGUA",
    "TEM_ENERGIA",
    "TEM_ESGOTO",
    "TEM_BANHEIRO",
    "TEM_BIBLIOTECA",
    "TEM_LAB_INFO",
    "TEM_QUADRA",
    "TEM_INTERNET",
]

# Raw é todo string; o schema explícito evita inferSchema e fixa o contrato.
RAW_COLS = [
    "ID_UNIDADE",
    "NOME_UNIDADE",
    "UF",
    "COD_UF",
    "NOME_MUNICIPIO",
    "COD_MUNICIPIO",
    "SETOR",
    "AREA",
    "SITUACAO",
    "LATITUDE",
    "LONGITUDE",
    "QT_SALAS",
    *INDICATOR_COLS,
]
RAW_SCHEMA = StructType([StructField(c, StringType()) for c in RAW_COLS])


def main():
    """Lê o raw das escolas, aplica os tipos reais e grava a bronze."""
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
        *[ct.yes_no_to_int(raw[c]).alias(c.lower()) for c in INDICATOR_COLS],
    ).filter(ct.zero_pad_code(raw["ID_UNIDADE"], 8).isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r11/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

# Colunas de contagem de matrícula: todas viram int no bronze.
COUNT_COLS = [
    "MAT_BASICA",
    "MAT_INFANTIL",
    "MAT_FUNDAMENTAL",
    "MAT_MEDIO",
    "MAT_PROFISSIONAL",
    "MAT_EJA",
    "MAT_ESPECIAL",
]

RAW_SCHEMA = StructType(
    [StructField("ID_UNIDADE", StringType())]
    + [StructField(c, StringType()) for c in COUNT_COLS]
)


def main():
    """Lê o raw das matrículas e tipa as contagens como inteiro."""
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    # Ausência de contagem fica como null (nunca 0): o cast de string vazia para
    # int já devolve null, preservando a semântica de ausência.
    bronze = raw.select(
        ct.zero_pad_code(raw["ID_UNIDADE"], 8).alias("id_unidade"),
        *[raw[c].cast("int").alias(c.lower()) for c in COUNT_COLS],
    ).filter(ct.zero_pad_code(raw["ID_UNIDADE"], 8).isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r11/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
FEDERAL_SETOR = "1"
ACTIVE_SITUACAO = "1"
# k=1 (célula + adjacentes imediatas); exact_ring=False inclui a própria célula.
KRING_K = 1
KRING_EXACT = False


def select_federal_active(df, level):
    """Filtra federais em atividade e anexa ponto WGS84 e célula H3 do nível.

    O ponto é reconstruído de latitude/longitude (WGS84) para servir tanto à
    indexação H3 quanto à distância geodésica — medidas sobre a coordenada
    original, sem reprojetar ida-e-volta.
    """
    is_federal = F.col("setor") == FEDERAL_SETOR
    is_active = F.col("situacao") == ACTIVE_SITUACAO
    fed = df.filter(is_federal & is_active)
    fed = dt.add_point_geometry(fed, "latitude", "longitude", "point")
    return dt.attach_h3_index(fed, "point", level, "h3_cell").select(
        "id_unidade", "enrollment", "h3_cell", "point"
    )


def summarize_cells(fed):
    """Contagem de escolas, total e média de matrículas por célula H3."""
    return fed.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("enrollment").cast("long").alias("total_enrollment"),
        F.round(F.avg("enrollment"), 2).alias("avg_enrollment"),
    )


def count_federal_neighbors(fed):
    """Nº de escolas vizinhas por célula (igual para todas as escolas da célula).

    Vizinhas de uma escola são as demais federais em atividade na própria célula
    ou nas adjacentes. Como o k-ring é o mesmo para toda a célula, esse total é
    ``(escolas na célula + adjacentes) - 1`` (a própria), idêntico para as escolas
    da célula.
    """
    cell_counts = fed.groupBy("h3_cell").agg(F.count(F.lit(1)).alias("cell_n"))
    ring = cell_counts.select(
        F.col("h3_cell").alias("center"),
        F.explode(stf.ST_H3KRing(F.col("h3_cell"), KRING_K, KRING_EXACT)).alias("cell"),
    )
    neighbor_counts = cell_counts.select(
        F.col("h3_cell").alias("cell"), F.col("cell_n").alias("neighbor_n")
    )
    pooled = ring.join(neighbor_counts, on="cell", how="left")
    return (
        pooled.groupBy("center")
        .agg(F.sum(F.coalesce("neighbor_n", F.lit(0))).alias("pool_n"))
        .select(
            F.col("center").alias("h3_cell"),
            (F.col("pool_n") - F.lit(1)).cast("long").alias("n_federal_neighbors"),
        )
    )


def average_pairwise_distances(fed):
    """Distâncias médias por célula: às vizinhas e à federal mais próxima (qualquer).

    Como as federais em atividade são poucas (~centenas), um cross join (N²) é
    barato. Para cada escola calcula-se a média até as vizinhas (célula +
    adjacentes) e o mínimo até qualquer outra federal; ambos são então
    promediados sobre as escolas da célula. Distância geodésica (km) sobre o ponto
    WGS84 original.
    """
    schools = fed.select(
        "id_unidade",
        "h3_cell",
        "point",
        stf.ST_H3KRing(F.col("h3_cell"), KRING_K, KRING_EXACT).alias("ring"),
    )
    left = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("point").alias("a_point"),
        F.col("ring").alias("a_ring"),
    )
    right = schools.select(
        F.col("id_unidade").alias("b_id"),
        F.col("h3_cell").alias("b_cell"),
        F.col("point").alias("b_point"),
    )
    pairs = (
        left.crossJoin(F.broadcast(right))
        .filter(F.col("a_id") != F.col("b_id"))
        .withColumn(
            "dist_km",
            stf.ST_DistanceSpheroid(F.col("a_point"), F.col("b_point")) / F.lit(1000.0),
        )
    )

    is_neighbor = F.array_contains(F.col("a_ring"), F.col("b_cell"))
    per_school = pairs.groupBy("a_id", "a_cell").agg(
        F.min("dist_km").alias("nearest_km"),
        F.avg(F.when(is_neighbor, F.col("dist_km"))).alias("neighbor_mean_km"),
    )
    return (
        per_school.groupBy("a_cell")
        .agg(
            F.round(F.avg("neighbor_mean_km"), 3).alias("avg_dist_neighbors_km"),
            F.round(F.avg("nearest_km"), 3).alias("avg_dist_nearest_km"),
        )
        .select(
            F.col("a_cell").alias("h3_cell"),
            "avg_dist_neighbors_km",
            "avg_dist_nearest_km",
        )
    )


def add_cell_geometry(df):
    """Reconstrói o polígono da célula H3 (4326) e projeta para 3857."""
    with_geom = df.withColumn(
        "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
    )
    return dt.to_web_mercator(with_geom)


def main():
    """Grade H3 nível 5 das escolas federais em atividade, com métricas de vizinhança."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    geo = spark.read.format("geoparquet").load(ORIGEM)
    fed = select_federal_active(geo, H3_LEVEL).cache()

    grid = (
        summarize_cells(fed)
        .join(count_federal_neighbors(fed), on="h3_cell", how="left")
        .join(average_pairwise_distances(fed), on="h3_cell", how="left")
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

# === /d/runs/C5-final-20260731-235945-r11/workdir/apps/educacao/jobs/silver/educacao_escola_matricula_geo.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula_geo", "silver")


def attach_enrollment(escola, matricula):
    """Anexa a matrícula da educação básica a cada escola (left join sem fan-out).

    A chave ``id_unidade`` é única em matrícula; ``join_no_fanout`` valida isso e
    falha cedo caso surja duplicidade, em vez de multiplicar linhas.
    """
    enrollment = matricula.select(
        "id_unidade", F.col("mat_basica").alias("enrollment")
    )
    return dt.join_no_fanout(escola, enrollment, on="id_unidade", how="left")


def main():
    """Escolas georreferenciadas com matrícula: ponto WGS84 preservado, geometria 3857."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA)

    geo = (
        attach_enrollment(escola, matricula)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        # latitude/longitude seguem para o gold porque as distâncias geodésicas são
        # medidas sobre o ponto WGS84 ORIGINAL — a geometria 3857 é só convenção de saída.
        .select(
            "id_unidade",
            "uf",
            "nome_municipio",
            "setor",
            "situacao",
            "enrollment",
            "latitude",
            "longitude",
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_matricula_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

