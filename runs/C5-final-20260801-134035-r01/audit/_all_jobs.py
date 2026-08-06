# === /d/runs/C5-final-20260801-134035-r01/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# Schema do dado cru: nomes na caixa original da fonte; a padronização para
# snake_case acontece nos alias do select do bronze.
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

# Indicadores de infraestrutura (0/1) — mesmo tratamento para todos.
INDICATOR_COLUMNS = {
    "TEM_AGUA": "tem_agua",
    "TEM_ENERGIA": "tem_energia",
    "TEM_ESGOTO": "tem_esgoto",
    "TEM_BANHEIRO": "tem_banheiro",
    "TEM_BIBLIOTECA": "tem_biblioteca",
    "TEM_LAB_INFO": "tem_lab_info",
    "TEM_QUADRA": "tem_quadra",
    "TEM_INTERNET": "tem_internet",
}


def main():
    """Lê o raw das escolas do Censo Escolar, limpa/tipa colunas e salva a bronze."""
    spark = SparkSession.builder.appName("educacao-escola-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        # chave normalizada com zero à esquerda, para casar com a matrícula
        ct.zero_pad_code(F.col("ID_UNIDADE"), 8).alias("id_unidade"),
        ct.normalize_text(F.col("NOME_UNIDADE")).alias("nome_unidade"),
        ct.standardize_uf(F.col("UF")).alias("uf"),
        ct.digits_only(F.col("COD_UF")).alias("cod_uf"),
        ct.normalize_text(F.col("NOME_MUNICIPIO")).alias("nome_municipio"),
        ct.digits_only(F.col("COD_MUNICIPIO")).alias("cod_municipio"),
        ct.blank_to_null(F.col("SETOR")).alias("setor"),
        ct.blank_to_null(F.col("AREA")).alias("area"),
        ct.blank_to_null(F.col("SITUACAO")).alias("situacao"),
        ct.to_coordinate(F.col("LATITUDE")).alias("latitude"),
        ct.to_coordinate(F.col("LONGITUDE")).alias("longitude"),
        F.col("QT_SALAS").cast("int").alias("qt_salas"),
        *[
            ct.yes_no_to_int(F.col(src)).alias(dst)
            for src, dst in INDICATOR_COLUMNS.items()
        ],
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-134035-r01/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

# Schema do dado cru: nomes na caixa original da fonte.
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

# Contagens de matrícula (mesmo cast inteiro para todas).
ENROLLMENT_COLUMNS = {
    "MAT_BASICA": "mat_basica",
    "MAT_INFANTIL": "mat_infantil",
    "MAT_FUNDAMENTAL": "mat_fundamental",
    "MAT_MEDIO": "mat_medio",
    "MAT_PROFISSIONAL": "mat_profissional",
    "MAT_EJA": "mat_eja",
    "MAT_ESPECIAL": "mat_especial",
}


def main():
    """Lê o raw das matrículas, tipa as contagens como inteiro e salva a bronze."""
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
            F.col(src).cast("int").alias(dst)
            for src, dst in ENROLLMENT_COLUMNS.items()
        ],
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-134035-r01/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
NEIGHBOR_RING = 1  # k-anel 1: própria célula + células imediatamente adjacentes
FEDERAL_SETOR = "1"
SITUACAO_ATIVA = "1"
METERS_PER_KM = 1000.0


def select_active_federal_schools(df: DataFrame) -> DataFrame:
    """Mantém apenas as escolas federais em atividade e indexa por célula H3.

    Constrói o ponto WGS84 (``point_wgs84``) a partir das coordenadas originais —
    base tanto do índice H3 quanto das distâncias geodésicas — e anexa a célula H3
    do nível pedido.
    """
    is_federal = F.col("setor") == FEDERAL_SETOR
    is_active = F.col("situacao") == SITUACAO_ATIVA
    return (
        df.filter(is_federal & is_active)
        .transform(
            lambda d: dt.add_point_geometry(d, "latitude", "longitude", "point_wgs84")
        )
        .transform(lambda d: dt.attach_h3_index(d, "point_wgs84", H3_LEVEL, "h3_cell"))
    )


def enrich_with_distance_stats(federal: DataFrame) -> DataFrame:
    """Anexa a cada escola as estatísticas de vizinhança e da federal mais próxima.

    Vizinhas são as outras federais ativas na mesma célula ou em célula adjacente
    (k-anel ``NEIGHBOR_RING``); ``n_neighbors`` é idêntico para toda a célula.
    Escolas sem vizinha recebem contagem 0 (ausência de vizinha conta como zero),
    mas mantêm a distância média de vizinhas em ``NULL`` — ausência não é zero.
    """
    neighbors = dt.h3_neighbor_distance_stats(
        federal, "id_unidade", "h3_cell", "point_wgs84", NEIGHBOR_RING
    )
    nearest = dt.nearest_point_distance(federal, "id_unidade", "point_wgs84")
    return (
        federal.select("id_unidade", "h3_cell", "total_enrollment")
        .join(neighbors, "id_unidade", "left")
        .join(nearest, "id_unidade", "left")
        .withColumn("n_neighbors", F.coalesce(F.col("n_neighbors"), F.lit(0)))
    )


def main():
    """Grade H3 nível 5 das escolas federais em atividade, com métricas de vizinhança."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("geoparquet").load(ORIGEM)

    federal = select_active_federal_schools(silver)
    enriched = enrich_with_distance_stats(federal)

    grid_aggregations = [
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("total_enrollment").cast("long").alias("total_enrollment"),
        F.round(F.avg("total_enrollment"), 2).alias("avg_enrollment"),
        # constante por célula: basta um valor qualquer do grupo
        F.max("n_neighbors").cast("long").alias("n_federal_neighbors"),
        F.round(F.avg("avg_dist_neighbors_m") / F.lit(METERS_PER_KM), 3).alias(
            "avg_dist_neighbors_km"
        ),
        F.round(F.avg("dist_nearest_m") / F.lit(METERS_PER_KM), 3).alias(
            "avg_dist_nearest_km"
        ),
    ]

    grid = (
        dt.aggregate_by_h3(enriched, "h3_cell", grid_aggregations)
        # ST_H3ToGeom devolve o polígono em EPSG:4326; persiste-se em 3857 (convenção)
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

# === /d/runs/C5-final-20260801-134035-r01/workdir/apps/educacao/jobs/silver/educacao_escola_matricula_geo.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula_geo", "silver")


def main():
    """Escolas com coordenada válida enriquecidas com o total de matrículas.

    Mantém lat/long WGS84 originais (para os cálculos geodésicos e H3 do gold) e
    persiste a geometria de ponto em EPSG:3857 (convenção de saída). Guarda
    setor/situação para os recortes do gold (ex.: federais em atividade).
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA).select(
        "id_unidade", "uf", "setor", "situacao", "latitude", "longitude"
    )
    # Só o total da educação básica interessa como "matrículas" desta tabela.
    matricula = spark.read.parquet(MATRICULA).select(
        "id_unidade", F.col("mat_basica").alias("total_enrollment")
    )

    geo = (
        dt.join_no_fanout(escola, matricula, "id_unidade", "left")
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
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
    print(f">> silver educacao_escola_matricula_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

