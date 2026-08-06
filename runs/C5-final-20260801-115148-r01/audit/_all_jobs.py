# === /d/runs/C5-final-20260801-115148-r01/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# Indicadores de infraestrutura (0/1) que viram int.
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
    """Lê o raw das escolas, tipa as colunas e salva a bronze.

    O CSV traz os nomes originais no cabeçalho (e uma coluna ``ano`` extra fora do
    contrato); referenciamos as colunas pelo nome e padronizamos para snake_case
    no ``select``. Lat/long viram ``double`` (nunca string, para não perder
    precisão nos cálculos geoespaciais).
    """
    spark = SparkSession.builder.appName("educacao-escola-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = spark.read.option("header", True).option("sep", ";").csv(ORIGEM)

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
        *[raw[c.upper()].cast("int").alias(c) for c in INDICADORES],
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-115148-r01/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
from pyspark.sql import SparkSession

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

# Colunas de contagem de matrículas, na ordem do contrato bronze.
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
    """Lê o raw das matrículas, tipa as contagens como int e salva a bronze."""
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = spark.read.option("header", True).option("sep", ";").csv(ORIGEM)

    bronze = raw.select(
        ct.zero_pad_code(raw["ID_UNIDADE"], 8).alias("id_unidade"),
        *[raw[c.upper()].cast("int").alias(c) for c in CONTAGENS],
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-115148-r01/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import column_transforms as ct
from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
METERS_PER_KM = 1000.0


def add_wgs84_point(df: DataFrame) -> DataFrame:
    """Reconstrói o ponto em WGS84 a partir de lat/long originais.

    A geometria persistida no silver está em EPSG:3857; para medir distâncias e
    indexar em H3 precisamos das coordenadas geográficas originais, sem o
    round-trip de reprojeção que perderia precisão.
    """
    return df.withColumn(
        "point_wgs84", ct.make_point(F.col("longitude"), F.col("latitude"))
    )


def main():
    """Grade H3 nível 5 das escolas federais em atividade.

    Por célula: nº de escolas, total e média de matrículas, nº de escolas vizinhas
    (na própria célula ou nas adjacentes — igual para toda a célula), distância
    média de cada escola às suas vizinhas e distância média de cada escola à
    federal mais próxima (qualquer, sem restrição de célula). As distâncias são
    geodésicas (``ST_DistanceSpheroid``) sobre as coordenadas WGS84 originais; só
    a geometria da célula é persistida em EPSG:3857.
    """
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = (
        spark.read.format("geoparquet").load(ORIGEM)
        .transform(add_wgs84_point)
        .transform(lambda d: dt.attach_h3_index(d, "point_wgs84", H3_LEVEL, "h3_cell"))
        .transform(
            lambda d: dt.add_nearest_neighbor_distance(
                d, "id_unidade", "point_wgs84", "nearest_dist_m"
            )
        )
        .transform(
            lambda d: dt.add_h3_ring_neighbor_stats(
                d, "id_unidade", "h3_cell", "point_wgs84", 1,
                "n_ring_neighbors", "avg_ring_dist_m",
            )
        )
    )

    grid = dt.aggregate_by_h3(
        schools,
        "h3_cell",
        [
            F.count(F.lit(1)).cast("long").alias("n_schools"),
            F.sum("total_enrollment").cast("long").alias("total_enrollment"),
            F.round(F.avg("total_enrollment"), 2).alias("avg_enrollment"),
            # a contagem de vizinhas é constante dentro da célula (depende só dela)
            F.max("n_ring_neighbors").cast("long").alias("n_federal_neighbors"),
            F.round(F.avg("avg_ring_dist_m") / METERS_PER_KM, 3).alias(
                "avg_dist_neighbors_km"
            ),
            F.round(F.avg("nearest_dist_m") / METERS_PER_KM, 3).alias(
                "avg_dist_nearest_km"
            ),
        ],
    ).transform(dt.to_web_mercator)

    grid = grid.select(
        "h3_cell",
        "n_schools",
        "total_enrollment",
        "avg_enrollment",
        "n_federal_neighbors",
        "avg_dist_neighbors_km",
        "avg_dist_nearest_km",
        "geometry",
    )

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-115148-r01/workdir/apps/educacao/jobs/silver/educacao_escola_geo.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_geo", "silver")

SETOR_FEDERAL = "1"
SITUACAO_ATIVA = "1"


def filter_federal_active(df):
    """Mantém apenas escolas do setor federal em atividade (SETOR=1, SITUACAO=1)."""
    is_federal = F.col("setor") == SETOR_FEDERAL
    is_active = F.col("situacao") == SITUACAO_ATIVA
    return df.filter(is_federal & is_active)


def main():
    """Escolas federais em atividade, georreferenciadas e com o total de matrículas.

    Filtra federais ativas com coordenada válida, junta o total de matrículas da
    educação básica (``mat_basica``) por unidade e persiste o ponto em EPSG:3857.
    ``latitude``/``longitude`` originais (WGS84) seguem no schema porque as
    distâncias do gold são medidas sobre as coordenadas originais, não sobre a
    projeção métrica.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA)

    federal = (
        escola.transform(filter_federal_active)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
    )
    enrollment = matricula.select(
        F.col("id_unidade"), F.col("mat_basica").alias("total_enrollment")
    )

    geo = (
        dt.join_no_fanout(federal, enrollment, "id_unidade", "left")
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "uf",
            "total_enrollment",
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

