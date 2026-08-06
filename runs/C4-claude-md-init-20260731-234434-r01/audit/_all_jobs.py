# === /d/runs/C4-claude-md-init-20260731-234434-r01/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.educacao.utils import column_transforms as educacao_ct
from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# Esquema do raw: todas as colunas como texto, na ordem em que aterrissam no CSV.
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

# Flags de infraestrutura ("0"/"1") convertidas para inteiro de uma vez.
FLAGS = [
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
    """Lê o raw das escolas (Censo Escolar) e salva a bronze saneada."""
    spark = SparkSession.builder.appName("educacao-escola-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.upper(F.trim(F.col("ID_UNIDADE"))).alias("id_unidade"),
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
        F.col("QT_SALAS").cast("int").alias("qt_salas"),
        *[F.col(c.upper()).cast("int").alias(c) for c in FLAGS],
    ).filter(educacao_ct.is_valid_school_id(F.col("id_unidade")))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C4-claude-md-init-20260731-234434-r01/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.educacao.utils import column_transforms as educacao_ct
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

# Colunas de contagem de matrículas por etapa (texto -> inteiro longo).
COUNTS = [
    "mat_basica",
    "mat_infantil",
    "mat_fundamental",
    "mat_medio",
    "mat_profissional",
    "mat_eja",
    "mat_especial",
]


def main():
    """Lê o raw das matrículas por escola e salva a bronze com contagens tipadas."""
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.upper(F.trim(F.col("ID_UNIDADE"))).alias("id_unidade"),
        *[F.col(c.upper()).cast("long").alias(c) for c in COUNTS],
    ).filter(educacao_ct.is_valid_school_id(F.col("id_unidade")))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C4-claude-md-init-20260731-234434-r01/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

H3_LEVEL = 5


def main():
    """Grade H3 (nível 5) das escolas federais em atividade e suas matrículas.

    Por célula: contagem de escolas, matrículas totais/médias, e três medidas de
    vizinhança calculadas escola a escola e depois agregadas na célula:

    - ``n_federal_neighbors``: nº de escolas vizinhas — as outras federais ativas
      na própria célula ou nas células imediatamente adjacentes (H3 k-ring 1).
      É idêntico para toda escola da célula, então basta o primeiro valor.
    - ``avg_dist_neighbors_km``: média, sobre as escolas da célula, da distância
      média de cada escola às suas vizinhas.
    - ``avg_dist_nearest_km``: média, sobre as escolas da célula, da distância de
      cada escola à federal mais próxima (qualquer, sem restrição de célula).

    As distâncias são geodésicas (``ST_DistanceSpheroid`` sobre o ponto WGS84
    original); o H3 opera em EPSG:4326 e a geometria da célula é reprojetada para
    EPSG:3857, o CRS planar do lake.
    """
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("geoparquet").load(ORIGEM)

    # Célula H3 (o H3 opera em EPSG:4326, então usamos o ponto lon/lat original) e
    # o disco k=1 (célula + adjacentes) que define a vizinhança de cada escola.
    escolas = (
        silver.withColumn("_pt4326", stc.ST_Point(F.col("longitude"), F.col("latitude")))
        .withColumn(
            "h3_cell", F.element_at(stf.ST_H3CellIDs(F.col("_pt4326"), H3_LEVEL, False), 1)
        )
        .withColumn("h3_disk", stf.ST_H3KRing(F.col("h3_cell"), F.lit(1), F.lit(False)))
        .select("id_unidade", "total_matriculas", "latitude", "longitude", "h3_cell", "h3_disk")
    ).cache()

    # Lado direito do produto cartesiano (colunas renomeadas p/ evitar ambiguidade).
    outras = escolas.select(
        F.col("id_unidade").alias("b_id"),
        F.col("latitude").alias("b_lat"),
        F.col("longitude").alias("b_lon"),
        F.col("h3_cell").alias("b_cell"),
    )

    dist_m = stf.ST_DistanceSpheroid(
        stc.ST_Point(F.col("longitude"), F.col("latitude")),
        stc.ST_Point(F.col("b_lon"), F.col("b_lat")),
    )
    is_other = F.col("id_unidade") != F.col("b_id")
    # Vizinha: outra escola cuja célula está no disco k=1 desta escola.
    is_neighbor = is_other & F.array_contains(F.col("h3_disk"), F.col("b_cell"))

    per_school = (
        escolas.crossJoin(outras)
        .withColumn("_dist_m", dist_m)
        .groupBy("id_unidade")
        .agg(
            F.first("h3_cell").alias("h3_cell"),
            F.first("total_matriculas").alias("total_matriculas"),
            F.min(F.when(is_other, F.col("_dist_m"))).alias("nearest_m"),
            F.avg(F.when(is_neighbor, F.col("_dist_m"))).alias("avg_neighbor_m"),
            F.sum(F.when(is_neighbor, F.lit(1)).otherwise(F.lit(0))).alias("n_neighbors"),
        )
    )

    grid = (
        per_school.groupBy("h3_cell")
        .agg(
            F.count(F.lit(1)).alias("n_schools"),
            F.sum("total_matriculas").alias("total_enrollment"),
            F.round(F.avg("total_matriculas"), 2).alias("avg_enrollment"),
            # idêntico para toda escola da célula -> primeiro valor basta
            F.first("n_neighbors").cast("long").alias("n_federal_neighbors"),
            F.round(F.avg("avg_neighbor_m") / F.lit(1000.0), 3).alias("avg_dist_neighbors_km"),
            F.round(F.avg("nearest_m") / F.lit(1000.0), 3).alias("avg_dist_nearest_km"),
        )
        .withColumn(
            "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
        )
        .transform(lambda d: dt.reproject_geometry(d, "geometry", "EPSG:4326", "EPSG:3857"))
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

# === /d/runs/C4-claude-md-init-20260731-234434-r01/workdir/apps/educacao/jobs/silver/educacao_escola_matricula_geo.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from apps.educacao.utils import column_transforms as educacao_ct
from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula_geo", "silver")


def main():
    """Escolas federais em atividade, georreferenciadas e com total de matrículas.

    Filtra o universo de interesse (setor federal, situação em atividade,
    coordenadas válidas), junta o total de matrículas da educação básica e
    materializa o ponto em EPSG:3857. Mantém latitude/longitude WGS84 para os
    cálculos geodésicos da camada gold (distância sobre o elipsoide, sem
    round-trip de projeção).
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA)

    federais = escola.filter(
        educacao_ct.is_federal(F.col("setor"))
        & educacao_ct.is_active(F.col("situacao"))
    ).transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))

    geo = (
        # matricula é única por id_unidade — join_no_fanout falha alto se não for.
        dt.join_no_fanout(
            federais,
            matricula.select("id_unidade", "mat_basica"),
            on="id_unidade",
            how="left",
        )
        .withColumn(
            "total_matriculas", F.coalesce(F.col("mat_basica"), F.lit(0)).cast("long")
        )
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "nome_unidade",
            "uf",
            "nome_municipio",
            "total_matriculas",
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

