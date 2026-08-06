# === /d/runs/C5-final-20260731-235945-r03/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# O schema segue a ORDEM das colunas do CSV (o header é ignorado ao casar com o
# schema explícito). Tudo entra como texto e é tipado no select do bronze.
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

# Indicadores de infraestrutura (0/1) tipados em bloco como inteiros.
INFRA_COLS = {
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
    """Lê o raw das escolas, tipa as colunas e salva a bronze."""
    spark = SparkSession.builder.appName("educacao-escola-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    infra = [ct.yes_no_to_int(raw[src]).alias(dst) for dst, src in INFRA_COLS.items()]
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
        *infra,
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r03/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
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

# Colunas de contagem: texto -> inteiro. Vazio vira NULL (ausência), não zero.
COUNT_COLS = {
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

    counts = [raw[src].cast("int").alias(dst) for dst, src in COUNT_COLS.items()]
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

# === /d/runs/C5-final-20260731-235945-r03/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5
NEIGHBOR_K = 1  # vizinhas = própria célula (k=0) + células adjacentes (anel k=1)
M_PER_KM = 1000.0


def build_schools(geo):
    """Reconstrói o ponto WGS84 e indexa cada escola na célula H3 nível 5.

    A distância geodésica e o índice H3 são calculados sobre a coordenada
    original (WGS84), não sobre a geometria projetada em 3857 do silver.
    """
    return (
        geo.withColumn("point_wgs", stc.ST_Point(F.col("longitude"), F.col("latitude")))
        .transform(lambda d: dt.attach_h3_index(d, "point_wgs", H3_LEVEL, "h3_cell"))
        .select("id_unidade", "enrollment", "h3_cell", "point_wgs")
    )


def neighbor_stats(schools):
    """Por escola: nº de vizinhas e distância média (m) até elas.

    Vizinhas = outras escolas na própria célula ou nas adjacentes (disco H3
    ``k=1``). O disco é expandido por ``ST_H3KRing`` e cruzado com as escolas
    situadas nas células resultantes.
    """
    disk = schools.select(
        F.col("id_unidade").alias("s_id"),
        F.col("point_wgs").alias("s_point"),
        F.explode(
            stf.ST_H3KRing(F.col("h3_cell"), F.lit(NEIGHBOR_K), F.lit(False))
        ).alias("disk_cell"),
    )
    is_neighbor = (F.col("disk_cell") == F.col("t.h3_cell")) & (
        F.col("s_id") != F.col("t.id_unidade")
    )
    pairs = disk.join(schools.alias("t"), is_neighbor, "inner").select(
        "s_id",
        stf.ST_DistanceSpheroid(F.col("s_point"), F.col("t.point_wgs")).alias("dist_m"),
    )
    return pairs.groupBy("s_id").agg(
        F.count(F.lit(1)).cast("long").alias("n_neighbors"),
        F.avg("dist_m").alias("avg_dist_neighbors_m"),
    )


def nearest_stats(schools):
    """Por escola: distância (m) até a federal mais próxima, sem restrição de célula."""
    left = schools.select(
        F.col("id_unidade").alias("s_id"), F.col("point_wgs").alias("s_point")
    )
    right = schools.select(
        F.col("id_unidade").alias("t_id"), F.col("point_wgs").alias("t_point")
    )
    pairs = (
        left.crossJoin(F.broadcast(right))
        .filter(F.col("s_id") != F.col("t_id"))
        .select(
            "s_id",
            stf.ST_DistanceSpheroid(F.col("s_point"), F.col("t_point")).alias("dist_m"),
        )
    )
    return pairs.groupBy("s_id").agg(F.min("dist_m").alias("nearest_dist_m"))


def enrich_schools(schools):
    """Anexa a cada escola as estatísticas de vizinhança e de vizinha mais próxima."""
    neighbors = neighbor_stats(schools).withColumnRenamed("s_id", "id_unidade")
    nearest = nearest_stats(schools).withColumnRenamed("s_id", "id_unidade")
    with_neighbors = dt.join_no_fanout(
        schools.select("id_unidade", "h3_cell", "enrollment"),
        neighbors,
        on="id_unidade",
        how="left",
    )
    return dt.join_no_fanout(with_neighbors, nearest, on="id_unidade", how="left")


def main():
    """Gold: agregação por célula H3 nível 5 das escolas federais em atividade."""
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = build_schools(spark.read.format("geoparquet").load(ORIGEM)).cache()
    per_school = enrich_schools(schools)

    agg_exprs = [
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("enrollment").cast("long").alias("total_enrollment"),
        F.avg("enrollment").alias("avg_enrollment"),
        F.coalesce(F.max("n_neighbors"), F.lit(0)).cast("long").alias("n_federal_neighbors"),
        (F.avg("avg_dist_neighbors_m") / F.lit(M_PER_KM)).alias("avg_dist_neighbors_km"),
        (F.avg("nearest_dist_m") / F.lit(M_PER_KM)).alias("avg_dist_nearest_km"),
    ]
    grid = dt.aggregate_by_h3(per_school, "h3_cell", agg_exprs).select(
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

# === /d/runs/C5-final-20260731-235945-r03/workdir/apps/educacao/jobs/silver/educacao_escola_matricula_geo.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from apps.educacao.utils import column_transforms as edu_ct
from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula_geo", "silver")


def keep_federal_active(escola):
    """Mantém apenas as escolas federais em atividade."""
    federal = edu_ct.is_federal(F.col("setor"))
    active = edu_ct.is_active(F.col("situacao"))
    return escola.filter(federal & active)


def main():
    """Silver: escolas federais em atividade, georreferenciadas e com matrículas.

    O total de matrículas vem de ``mat_basica`` (educação básica). O join é
    ``left`` para não descartar escola sem registro de matrícula — nesse caso
    ``enrollment`` fica ``NULL`` (ausência), nunca zero.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA).select(
        "id_unidade", F.col("mat_basica").alias("enrollment")
    )

    escola = (
        escola.transform(keep_federal_active)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
    )

    joined = dt.join_no_fanout(escola, matricula, on="id_unidade", how="left")

    geo = (
        joined.transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "uf",
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

