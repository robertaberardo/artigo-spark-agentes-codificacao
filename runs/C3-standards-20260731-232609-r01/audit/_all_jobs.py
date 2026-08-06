# === /d/runs/C3-standards-20260731-232609-r01/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.educacao.utils import column_transforms as edu_ct
from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# Schema posicional do CSV cru: mantém os nomes originais (caixa da fonte) e todas
# as colunas, inclusive as de infraestrutura (TEM_*) que não seguem para a bronze.
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
    """Lê o raw das escolas, tipa as colunas de interesse e salva a bronze."""
    spark = SparkSession.builder.appName("educacao-escola-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.filter(edu_ct.is_valid_school_id(F.col("ID_UNIDADE"))).select(
        ct.zero_pad_code(F.col("ID_UNIDADE"), 8).alias("id_unidade"),
        ct.normalize_text(F.col("NOME_UNIDADE")).alias("nome_unidade"),
        ct.standardize_uf(F.col("UF")).alias("uf"),
        ct.digits_only(F.col("COD_MUNICIPIO")).alias("municipio_ibge"),
        F.col("SETOR").cast("int").alias("setor"),
        F.col("AREA").cast("int").alias("area"),
        F.col("SITUACAO").cast("int").alias("situacao"),
        ct.to_coordinate(F.col("LATITUDE")).alias("latitude"),
        ct.to_coordinate(F.col("LONGITUDE")).alias("longitude"),
        F.col("QT_SALAS").cast("int").alias("qt_salas"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C3-standards-20260731-232609-r01/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.educacao.utils import column_transforms as edu_ct
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
    """Lê o raw das matrículas, tipa as quantidades e salva a bronze."""
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.filter(edu_ct.is_valid_school_id(F.col("ID_UNIDADE"))).select(
        ct.zero_pad_code(F.col("ID_UNIDADE"), 8).alias("id_unidade"),
        F.col("MAT_BASICA").cast("int").alias("mat_basica"),
        F.col("MAT_INFANTIL").cast("int").alias("mat_infantil"),
        F.col("MAT_FUNDAMENTAL").cast("int").alias("mat_fundamental"),
        F.col("MAT_MEDIO").cast("int").alias("mat_medio"),
        F.col("MAT_PROFISSIONAL").cast("int").alias("mat_profissional"),
        F.col("MAT_EJA").cast("int").alias("mat_eja"),
        F.col("MAT_ESPECIAL").cast("int").alias("mat_especial"),
    )

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C3-standards-20260731-232609-r01/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

# Recorte da análise: escolas federais em atividade (códigos do Censo Escolar).
SETOR_FEDERAL = 1
SITUACAO_ATIVA = 1

H3_LEVEL = 5
# Raio 1 do k-ring = própria célula + as seis células imediatamente adjacentes.
NEIGHBOR_RING = 1


def load_federal_active_schools(spark) -> DataFrame:
    """Escolas federais em atividade, com ponto WGS84 e célula H3 nível 5.

    O ponto é reconstruído a partir de latitude/longitude em WGS84 (não da
    geometria 3857 do silver) para que H3 e distâncias operem sobre as
    coordenadas geográficas originais.
    """
    is_federal = F.col("setor") == SETOR_FEDERAL
    is_active = F.col("situacao") == SITUACAO_ATIVA
    return (
        spark.read.format("geoparquet")
        .load(ORIGEM)
        .filter(is_federal & is_active)
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude", "point"))
        .transform(lambda d: dt.attach_h3_index(d, "point", H3_LEVEL, "h3_cell"))
        .select("id_unidade", "enrollment", "h3_cell", "point")
    )


def aggregate_cell_base(schools: DataFrame) -> DataFrame:
    """Métricas básicas por célula: contagem, matrículas totais e média por escola."""
    return schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("enrollment").cast("long").alias("total_enrollment"),
        F.round(F.avg("enrollment"), 2).alias("avg_enrollment"),
    )


def aggregate_nearest_federal(schools: DataFrame) -> DataFrame:
    """Distância média, por célula, de cada escola à federal mais próxima (global).

    A escola mais próxima é buscada entre todas as federais em atividade, sem
    restrição de célula; depois faz-se a média entre as escolas da célula.
    """
    with_nearest = dt.add_nearest_neighbor_distance(
        schools, "id_unidade", "point", "dist_nearest_m"
    )
    return with_nearest.groupBy("h3_cell").agg(
        F.avg("dist_nearest_m").alias("avg_dist_nearest_m")
    )


def build_neighborhood_members(schools: DataFrame) -> DataFrame:
    """Universo de vizinhança de cada célula: escolas da própria célula e das adjacentes.

    Expande o k-ring de raio 1 das células ocupadas e recasa as escolas pela sua
    própria célula, ligando cada célula central às escolas do seu entorno.
    """
    occupied = schools.select("h3_cell").distinct()
    pairs = dt.add_h3_kring(occupied, "h3_cell", NEIGHBOR_RING, "kring").select(
        F.col("h3_cell").alias("center_cell"),
        F.explode("kring").alias("member_cell"),
    )
    members = schools.select(
        F.col("id_unidade").alias("member_id"),
        F.col("h3_cell").alias("member_cell_h3"),
        F.col("point").alias("member_point"),
    )
    return pairs.join(
        members, pairs["member_cell"] == members["member_cell_h3"], "inner"
    ).select("center_cell", "member_id", "member_point")


def aggregate_neighbors(schools: DataFrame, members: DataFrame) -> DataFrame:
    """Por célula: nº de escolas vizinhas e distância média de cada escola às vizinhas.

    ``n_federal_neighbors`` é o tamanho do universo de vizinhança menos a própria
    escola — igual para toda a célula. A distância média percorre, para cada
    escola da célula, as suas vizinhas (própria célula + adjacentes) e, em seguida,
    faz a média entre as escolas da célula.
    """
    n_neighbors = members.groupBy("center_cell").agg(
        (F.countDistinct("member_id") - F.lit(1))
        .cast("long")
        .alias("n_federal_neighbors")
    )

    center_schools = schools.select(
        F.col("h3_cell").alias("center_cell"),
        F.col("id_unidade").alias("school_id"),
        F.col("point").alias("school_point"),
    )
    # distância geodésica sobre WGS84 (ST_DistanceSpheroid), não planar
    geodesic_dist = stf.ST_DistanceSpheroid(
        F.col("school_point"), F.col("member_point")
    )
    per_school = (
        center_schools.join(members, on="center_cell", how="inner")
        .filter(F.col("school_id") != F.col("member_id"))
        .withColumn("dist_m", geodesic_dist)
        .groupBy("center_cell", "school_id")
        .agg(F.avg("dist_m").alias("school_avg_dist_m"))
    )
    avg_neighbors = per_school.groupBy("center_cell").agg(
        F.avg("school_avg_dist_m").alias("avg_dist_neighbors_m")
    )

    return n_neighbors.join(avg_neighbors, on="center_cell", how="left").select(
        F.col("center_cell").alias("h3_cell"),
        "n_federal_neighbors",
        "avg_dist_neighbors_m",
    )


def assemble_grid(
    base: DataFrame, neighbors: DataFrame, nearest: DataFrame
) -> DataFrame:
    """Junta as métricas por célula, converte distâncias em km e monta a geometria."""
    cell_geom = F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
    return (
        base.join(neighbors, on="h3_cell", how="left")
        .join(nearest, on="h3_cell", how="left")
        .withColumn(
            "avg_dist_neighbors_km",
            F.round(F.col("avg_dist_neighbors_m") / F.lit(1000.0), 3),
        )
        .withColumn(
            "avg_dist_nearest_km",
            F.round(F.col("avg_dist_nearest_m") / F.lit(1000.0), 3),
        )
        .withColumn("geometry", cell_geom)
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


def main():
    """Grade H3 nível 5 das escolas federais em atividade: matrículas, vizinhança e distâncias."""
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # reusado em quatro agregações independentes; cache evita recalcular H3/ponto
    schools = load_federal_active_schools(spark).cache()

    base = aggregate_cell_base(schools)
    members = build_neighborhood_members(schools)
    neighbors = aggregate_neighbors(schools, members)
    nearest = aggregate_nearest_federal(schools)

    grid = assemble_grid(base, neighbors, nearest)

    grid.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C3-standards-20260731-232609-r01/workdir/apps/educacao/jobs/silver/educacao_escola_geo.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_geo", "silver")


def main():
    """Escolas georreferenciadas com matrículas: ponto, geometria 3857 e matrícula básica.

    Mantém latitude/longitude em WGS84 na saída porque as agregações de gold
    precisam medir distâncias geodésicas sobre as coordenadas originais, antes de
    qualquer projeção.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA).select(
        F.col("id_unidade"), F.col("mat_basica").alias("enrollment")
    )

    geo = (
        escola.transform(
            lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude")
        )
        .transform(lambda d: dt.join_no_fanout(d, matricula, "id_unidade", "left"))
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "uf",
            "municipio_ibge",
            "setor",
            "situacao",
            "latitude",
            "longitude",
            "enrollment",
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

