# === /d/runs/C5-final-20260801-122257-r01/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# Schema explícito na ordem exata do CSV (header em CAIXA ALTA); nunca inferir.
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

# Indicadores 0/1 do Censo Escolar; snake_case de saída -> nome do campo de origem.
INDICATORS = {
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
    """Lê o raw das escolas, tipa/normaliza as colunas e salva a bronze."""
    spark = SparkSession.builder.appName("educacao-escola-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    indicators = [
        ct.yes_no_to_int(raw[origem]).alias(destino)
        for destino, origem in INDICATORS.items()
    ]
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
        *indicators,
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-122257-r01/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

# Schema explícito na ordem exata do CSV (header em CAIXA ALTA); nunca inferir.
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

# Contagens de matrícula; snake_case de saída -> nome do campo de origem.
COUNTS = {
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
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    # Ausência de contagem fica NULL (nunca 0): distingue "sem informação" de "zero
    # matrículas" e não contamina somas/médias a jusante.
    counts = [raw[origem].cast("int").alias(destino) for destino, origem in COUNTS.items()]
    bronze = raw.select(
        ct.zero_pad_code(raw["ID_UNIDADE"], 8).alias("id_unidade"),
        *counts,
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-122257-r01/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
# k=1 (ST_H3KRing com exact_ring=False) = a própria célula + o anel imediatamente
# adjacente; é a definição de "vizinhança" pedida para as escolas.
NEIGHBOR_K = 1
M_PER_KM = 1000.0


def build_analysis_base(geo: DataFrame) -> DataFrame:
    """Ponto WGS84 original + célula H3 nível 5 de cada escola.

    O H3 é indexado sobre as coordenadas geográficas (não sobre a geometria 3857),
    condição para o id da célula ser correto.
    """
    return (
        geo.withColumn("geom_wgs84", stc.ST_Point(F.col("longitude"), F.col("latitude")))
        .transform(lambda d: dt.attach_h3_index(d, "geom_wgs84", H3_LEVEL, "h3_cell"))
        .select("id_unidade", "h3_cell", "geom_wgs84", "total_enrollment")
    )


def neighbor_distances(base: DataFrame) -> DataFrame:
    """Por escola: distância média às vizinhas e quantidade de vizinhas.

    Vizinhas = outras escolas na própria célula ou nas adjacentes (k-ring 1). O
    self-join espacial casa cada escola às escolas cuja célula está no seu k-ring,
    excluindo ela mesma; a distância é geodésica sobre os pontos WGS84 originais.
    """
    rings = base.select(
        F.col("id_unidade").alias("s_id"),
        F.col("geom_wgs84").alias("s_geom"),
        F.explode(
            stf.ST_H3KRing(F.col("h3_cell"), F.lit(NEIGHBOR_K), F.lit(False))
        ).alias("ring_cell"),
    )
    others = base.select(
        F.col("id_unidade").alias("t_id"),
        F.col("h3_cell").alias("t_cell"),
        F.col("geom_wgs84").alias("t_geom"),
    )
    is_neighbor = (F.col("ring_cell") == F.col("t_cell")) & (F.col("s_id") != F.col("t_id"))
    pairs = rings.join(others, is_neighbor, "inner").withColumn(
        "dist_m", stf.ST_DistanceSpheroid(F.col("s_geom"), F.col("t_geom"))
    )
    return pairs.groupBy("s_id").agg(
        F.avg("dist_m").alias("avg_dist_neighbors_m"),
        F.count(F.lit(1)).cast("long").alias("n_neighbors"),
    )


def nearest_federal(base: DataFrame) -> DataFrame:
    """Por escola: distância geodésica à federal mais próxima (qualquer célula).

    Produto cartesiano do conjunto (pequeno) de federais em atividade contra si
    mesmo, excluindo a própria escola; guarda a menor distância.
    """
    left = base.select(
        F.col("id_unidade").alias("s_id"), F.col("geom_wgs84").alias("s_geom")
    )
    right = base.select(
        F.col("id_unidade").alias("t_id"), F.col("geom_wgs84").alias("t_geom")
    )
    pairs = left.crossJoin(F.broadcast(right)).filter(F.col("s_id") != F.col("t_id"))
    with_dist = pairs.withColumn(
        "dist_m", stf.ST_DistanceSpheroid(F.col("s_geom"), F.col("t_geom"))
    )
    return with_dist.groupBy("s_id").agg(F.min("dist_m").alias("dist_nearest_m"))


def per_school_metrics(base: DataFrame) -> DataFrame:
    """Junta as métricas por escola; escolas sem vizinhas ficam com 0 vizinhas."""
    neighbors = neighbor_distances(base).withColumnRenamed("s_id", "id_unidade")
    nearest = nearest_federal(base).withColumnRenamed("s_id", "id_unidade")
    return (
        base.select("id_unidade", "h3_cell", "total_enrollment")
        .join(neighbors, "id_unidade", "left")
        .join(nearest, "id_unidade", "left")
        .withColumn("n_neighbors", F.coalesce(F.col("n_neighbors"), F.lit(0)))
    )


def grid_aggregations() -> list:
    """Agregações por célula H3.

    Vizinhas e distância média às vizinhas são idênticas para todas as escolas da
    mesma célula (compartilham o mesmo k-ring), então ``F.max``/``F.avg`` resumem a
    célula sem perda. A média de matrículas é total / nº de escolas.
    """
    return [
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("total_enrollment").cast("long").alias("total_enrollment"),
        (F.sum("total_enrollment") / F.count(F.lit(1))).cast("double").alias("avg_enrollment"),
        F.max("n_neighbors").cast("long").alias("n_federal_neighbors"),
        (F.avg("avg_dist_neighbors_m") / F.lit(M_PER_KM)).alias("avg_dist_neighbors_km"),
        (F.avg("dist_nearest_m") / F.lit(M_PER_KM)).alias("avg_dist_nearest_km"),
    ]


FINAL_COLUMNS = [
    "h3_cell",
    "n_schools",
    "total_enrollment",
    "avg_enrollment",
    "n_federal_neighbors",
    "avg_dist_neighbors_km",
    "avg_dist_nearest_km",
    "geometry",
]


def main():
    """Agrega as escolas federais ativas por célula H3 nível 5 (gold)."""
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    base = spark.read.format("geoparquet").load(ORIGEM).transform(build_analysis_base)

    grid = (
        per_school_metrics(base)
        .transform(lambda d: dt.aggregate_by_h3(d, "h3_cell", grid_aggregations()))
        .transform(dt.to_web_mercator)
        .select(*FINAL_COLUMNS)
    )

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-122257-r01/workdir/apps/educacao/jobs/silver/educacao_escola_matricula.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula", "silver")

# Códigos do Censo Escolar: setor 1 = federal; situação 1 = em atividade.
SETOR_FEDERAL = "1"
SITUACAO_ATIVA = "1"


def keep_federal_active(df):
    """Mantém só escolas federais em atividade — universo de toda a análise."""
    is_federal = F.col("setor") == SETOR_FEDERAL
    is_active = F.col("situacao") == SITUACAO_ATIVA
    return df.filter(is_federal & is_active)


def main():
    """Consolida escola + matrícula (federais em atividade) e georreferencia.

    Preserva latitude/longitude WGS84 originais além da geometria em 3857: o gold
    mede distâncias geodésicas sobre o ponto original e indexa o H3 em coordenadas
    geográficas, sem depender do round-trip da projeção.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA)

    consolidado = (
        escola.transform(keep_federal_active)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .transform(
            lambda d: dt.join_lookup(
                d, matricula, on="id_unidade", columns=["mat_basica"], how="left"
            )
        )
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "nome_unidade",
            "uf",
            "cod_municipio",
            "setor",
            "situacao",
            "latitude",
            "longitude",
            F.col("mat_basica").alias("total_enrollment"),
            "geometry",
        )
    )

    consolidado.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_matricula: {n} escolas federais ativas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

