# === /d/runs/C5-final-20260731-235945-r04/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# O CSV do censo escolar aterrissa com todas as colunas como texto e separador ";".
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

# ID_UNIDADE do censo é um código de 8 dígitos; chaves fora desse formato são lixo.
VALID_KEY = r"^[0-9]{8}$"


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

    id_unidade = ct.zero_pad_code(raw["ID_UNIDADE"], 8)
    bronze = raw.select(
        id_unidade.alias("id_unidade"),
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
    ).filter(id_unidade.rlike(VALID_KEY))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r04/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

# O CSV do censo aterrissa com contagens em texto e separador ";".
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

VALID_KEY = r"^[0-9]{8}$"


def main():
    """Lê o raw de matrículas, tipa as contagens e salva a bronze."""
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    id_unidade = ct.zero_pad_code(raw["ID_UNIDADE"], 8)
    bronze = raw.select(
        id_unidade.alias("id_unidade"),
        raw["MAT_BASICA"].cast("int").alias("mat_basica"),
        raw["MAT_INFANTIL"].cast("int").alias("mat_infantil"),
        raw["MAT_FUNDAMENTAL"].cast("int").alias("mat_fundamental"),
        raw["MAT_MEDIO"].cast("int").alias("mat_medio"),
        raw["MAT_PROFISSIONAL"].cast("int").alias("mat_profissional"),
        raw["MAT_EJA"].cast("int").alias("mat_eja"),
        raw["MAT_ESPECIAL"].cast("int").alias("mat_especial"),
    ).filter(id_unidade.rlike(VALID_KEY))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r04/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_federal_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
# Vizinhança: própria célula + as imediatamente adjacentes (1 anel do ST_H3KRing).
K_RING = 1
METERS_PER_KM = 1000.0


def prepare_schools(df):
    """Recria o ponto WGS84 (para distância geodésica) e anexa a célula H3 nível 5.

    A geometria persistida no silver está em EPSG:3857; medir distância exige
    voltar ao WGS84 original, então o ponto é reconstruído a partir de
    latitude/longitude (sem round-trip de projeção), que também alimenta o H3.
    """
    return (
        df.transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude", "point"))
        .transform(lambda d: dt.attach_h3_index(d, "point", H3_LEVEL, "h3_cell"))
        .select("id_unidade", "h3_cell", "enrollment", "point")
    )


def add_distance_stats(df):
    """Anexa, por escola, as estatísticas de vizinhança e do vizinho mais próximo."""
    return (
        df.transform(
            lambda d: dt.add_kring_neighbor_stats(
                d, "id_unidade", "h3_cell", "point", K_RING,
                "n_neighbors", "avg_dist_neighbors_m",
            )
        ).transform(
            lambda d: dt.add_nearest_neighbor_distance(
                d, "id_unidade", "point", "dist_nearest_m"
            )
        )
    )


def cell_aggregations():
    """Expressões de agregação por célula (construídas com Spark já ativo)."""
    return [
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("enrollment").cast("long").alias("total_enrollment"),
        F.round(F.avg("enrollment"), 2).alias("avg_enrollment"),
        # n_neighbors é constante na célula (depende só da célula), logo max = o valor.
        F.max("n_neighbors").cast("long").alias("n_federal_neighbors"),
        F.round(F.avg("avg_dist_neighbors_km"), 3).alias("avg_dist_neighbors_km"),
        F.round(F.avg("dist_nearest_km"), 3).alias("avg_dist_nearest_km"),
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
    """Grade H3 nível 5 das escolas federais em atividade, com matrículas e distâncias."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("geoparquet").load(ORIGEM)

    per_school = (
        silver.transform(prepare_schools)
        .transform(add_distance_stats)
        .select(
            "h3_cell",
            "enrollment",
            "n_neighbors",
            (F.col("avg_dist_neighbors_m") / METERS_PER_KM).alias("avg_dist_neighbors_km"),
            (F.col("dist_nearest_m") / METERS_PER_KM).alias("dist_nearest_km"),
        )
    )

    grid = (
        dt.aggregate_by_h3(per_school, "h3_cell", cell_aggregations())
        .transform(dt.to_web_mercator)
        .select(*FINAL_COLUMNS)
    )

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    grid.drop("geometry").orderBy(F.col("n_schools").desc()).show(10, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r04/workdir/apps/educacao/jobs/silver/educacao_escola_federal_geo.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_federal_geo", "silver")

# Códigos do censo escolar (ver metadata.toml).
SETOR_FEDERAL = "1"
SITUACAO_ATIVA = "1"


def select_federal_active(df):
    """Mantém apenas as escolas federais (setor 1) em atividade (situação 1)."""
    is_federal = F.col("setor") == SETOR_FEDERAL
    is_active = F.col("situacao") == SITUACAO_ATIVA
    return df.filter(is_federal & is_active)


def main():
    """Escolas federais em atividade, georreferenciadas e com matrículas.

    Preserva latitude/longitude (WGS84) para o cálculo geodésico no gold; a
    geometria de saída vai em EPSG:3857 por convenção do lake.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-federal-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA).select(
        F.col("id_unidade"), F.col("mat_basica").alias("enrollment")
    )

    geo = (
        escola.transform(select_federal_active)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .transform(
            lambda d: dt.join_no_fanout(d, matricula, "id_unidade", "left")
        )
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "nome_unidade",
            "uf",
            "enrollment",
            "latitude",
            "longitude",
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_federal_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

