# === /d/runs/C5-final-20260801-145412-r01/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# Schema explícito do raw (todas as colunas como string, ordem do cabeçalho).
RAW_SCHEMA = T.StructType(
    [
        T.StructField(name, T.StringType())
        for name in [
            "ID_UNIDADE", "NOME_UNIDADE", "UF", "COD_UF", "NOME_MUNICIPIO",
            "COD_MUNICIPIO", "SETOR", "AREA", "SITUACAO", "LATITUDE", "LONGITUDE",
            "QT_SALAS", "TEM_AGUA", "TEM_ENERGIA", "TEM_ESGOTO", "TEM_BANHEIRO",
            "TEM_BIBLIOTECA", "TEM_LAB_INFO", "TEM_QUADRA", "TEM_INTERNET",
        ]
    ]
)


def main():
    """Lê o raw das escolas, padroniza nomes/tipos e grava a bronze."""
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
        ct.normalize_text(F.col("NOME_MUNICIPIO")).alias("nome_municipio"),
        ct.blank_to_null(F.col("SETOR")).cast("int").alias("setor"),
        ct.blank_to_null(F.col("AREA")).cast("int").alias("area"),
        ct.blank_to_null(F.col("SITUACAO")).cast("int").alias("situacao"),
        ct.to_coordinate(F.col("LATITUDE")).alias("latitude"),
        ct.to_coordinate(F.col("LONGITUDE")).alias("longitude"),
        ct.blank_to_null(F.col("QT_SALAS")).cast("int").alias("qt_salas"),
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-145412-r01/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

RAW_SCHEMA = T.StructType(
    [
        T.StructField(name, T.StringType())
        for name in [
            "ID_UNIDADE", "MAT_BASICA", "MAT_INFANTIL", "MAT_FUNDAMENTAL",
            "MAT_MEDIO", "MAT_PROFISSIONAL", "MAT_EJA", "MAT_ESPECIAL",
        ]
    ]
)

# Colunas de contagem: mesmo alias em snake_case, todas casteadas para int.
COUNT_COLUMNS = {
    "MAT_BASICA": "mat_basica",
    "MAT_INFANTIL": "mat_infantil",
    "MAT_FUNDAMENTAL": "mat_fundamental",
    "MAT_MEDIO": "mat_medio",
    "MAT_PROFISSIONAL": "mat_profissional",
    "MAT_EJA": "mat_eja",
    "MAT_ESPECIAL": "mat_especial",
}


def main():
    """Lê o raw das matrículas, normaliza a chave e as contagens, grava a bronze."""
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
            ct.blank_to_null(F.col(src)).cast("int").alias(dst)
            for src, dst in COUNT_COLUMNS.items()
        ],
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-145412-r01/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import DataFrame, functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
SETOR_FEDERAL = 1
SITUACAO_ATIVA = 1


def load_federal_active_schools(spark) -> DataFrame:
    """Escolas federais em atividade, com ponto WGS84 e célula H3 de resolução 5.

    ``pt`` fica em WGS84 (a partir da lat/long originais) para a distância
    geodésica; a célula H3 é calculada em coordenadas geográficas.
    """
    geo = spark.read.format("geoparquet").load(ORIGEM)
    is_federal = F.col("setor") == SETOR_FEDERAL
    is_active = F.col("situacao") == SITUACAO_ATIVA
    return (
        geo.filter(is_federal & is_active)
        .withColumn("pt", stc.ST_Point(F.col("longitude"), F.col("latitude")))
        .withColumn("h3_cell", F.element_at(stf.ST_H3CellIDs(F.col("pt"), H3_LEVEL, False), 1))
        .select("id_unidade", "enrollment", "pt", "h3_cell")
    )


def cell_enrollment_stats(schools: DataFrame) -> DataFrame:
    """Contagem de escolas e estatísticas de matrículas por célula."""
    return schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("int").alias("n_schools"),
        F.sum("enrollment").cast("long").alias("total_enrollment"),
        F.avg("enrollment").alias("avg_enrollment"),
    )


def cell_neighbor_counts(schools: DataFrame) -> DataFrame:
    """Escolas na célula + adjacentes (disco k=1) — igual para toda a célula."""
    counts = schools.groupBy("h3_cell").agg(F.count(F.lit(1)).alias("cnt"))
    rings = schools.select("h3_cell").distinct().withColumn(
        "ring_cell", F.explode(stf.ST_H3KRing(F.col("h3_cell"), 1, False))
    )
    return (
        rings.alias("r")
        .join(counts.alias("c"), F.col("r.ring_cell") == F.col("c.h3_cell"), "left")
        .groupBy(F.col("r.h3_cell").alias("h3_cell"))
        .agg(F.sum(F.coalesce(F.col("c.cnt"), F.lit(0))).cast("int").alias("n_federal_neighbors"))
    )


def _neighbor_pairs(schools: DataFrame) -> DataFrame:
    """Pares (escola, vizinha) com a distância geodésica em km entre elas.

    Vizinha = outra escola cuja célula está no disco k=1 (própria + adjacentes)
    da célula da escola. A distância é medida sobre os pontos WGS84 originais.
    """
    a = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("pt").alias("a_pt"),
    ).withColumn("ring_cell", F.explode(stf.ST_H3KRing(F.col("a_cell"), 1, False)))
    b = schools.select(
        F.col("id_unidade").alias("b_id"),
        F.col("h3_cell").alias("b_cell"),
        F.col("pt").alias("b_pt"),
    )
    return (
        a.join(b, F.col("ring_cell") == F.col("b_cell"), "inner")
        .filter(F.col("a_id") != F.col("b_id"))
        .withColumn(
            "dist_km", stf.ST_DistanceSpheroid(F.col("a_pt"), F.col("b_pt")) / F.lit(1000.0)
        )
    )


def cell_neighbor_distances(schools: DataFrame) -> DataFrame:
    """Distância média de cada escola às vizinhas, agregada por célula."""
    per_school = _neighbor_pairs(schools).groupBy("a_id", "a_cell").agg(
        F.avg("dist_km").alias("school_avg_km")
    )
    return per_school.groupBy(F.col("a_cell").alias("h3_cell")).agg(
        F.avg("school_avg_km").alias("avg_dist_neighbors_km")
    )


def cell_nearest_distances(schools: DataFrame) -> DataFrame:
    """Distância média de cada escola à federal mais próxima (qualquer célula)."""
    a = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("pt").alias("a_pt"),
    )
    other = schools.select(
        F.col("id_unidade").alias("o_id"), F.col("pt").alias("o_pt")
    )
    per_school = (
        a.crossJoin(other)
        .filter(F.col("a_id") != F.col("o_id"))
        .withColumn(
            "dist_km", stf.ST_DistanceSpheroid(F.col("a_pt"), F.col("o_pt")) / F.lit(1000.0)
        )
        .groupBy("a_id", "a_cell")
        .agg(F.min("dist_km").alias("nearest_km"))
    )
    return per_school.groupBy(F.col("a_cell").alias("h3_cell")).agg(
        F.avg("nearest_km").alias("avg_dist_nearest_km")
    )


def main():
    """Grade H3 res.5 de escolas federais em atividade: densidade + distâncias."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = load_federal_active_schools(spark).cache()

    grid = (
        cell_enrollment_stats(schools)
        .join(cell_neighbor_counts(schools), "h3_cell", "left")
        .join(cell_neighbor_distances(schools), "h3_cell", "left")
        .join(cell_nearest_distances(schools), "h3_cell", "left")
        .withColumn("geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1))
        .transform(dt.to_web_mercator)
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

# === /d/runs/C5-final-20260801-145412-r01/workdir/apps/educacao/jobs/silver/educacao_escola_geo.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_geo", "silver")


def main():
    """Consolida escola + matrículas, georreferencia o ponto e grava a silver.

    Mantém ``latitude``/``longitude`` originais (WGS84) além da geometria em
    EPSG:3857: a medição de distância no gold é geodésica sobre o ponto original,
    nunca sobre o round-trip da projeção métrica.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA).select(
        "id_unidade", F.col("mat_basica").alias("enrollment")
    )

    enriched = dt.join_no_fanout(escola, matricula, on="id_unidade", how="left")

    geo = (
        enriched
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
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
    print(f">> silver educacao_escola_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

