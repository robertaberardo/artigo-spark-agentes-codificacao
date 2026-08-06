# === /d/runs/C5-final-20260731-235945-r09/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# O raw aterrissa como CSV (header, separador ";") com todas as colunas em texto.
# O arquivo traz ainda uma coluna final "ano" que não faz parte da chave do
# domínio; como o schema explícito cobre só as colunas declaradas, o Spark ignora
# a coluna extra à direita.
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
    """Lê o raw das escolas (Censo Escolar), padroniza nomes e tipa as colunas."""
    spark = SparkSession.builder.appName("educacao-escola-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.trim(F.col("ID_UNIDADE")).alias("id_unidade"),
        ct.normalize_text(F.col("NOME_UNIDADE")).alias("school_name"),
        ct.standardize_uf(F.col("UF")).alias("uf"),
        F.trim(F.col("COD_UF")).alias("uf_code"),
        ct.normalize_text(F.col("NOME_MUNICIPIO")).alias("municipality_name"),
        F.trim(F.col("COD_MUNICIPIO")).alias("municipality_code"),
        F.col("SETOR").cast("int").alias("admin_sector"),
        F.col("AREA").cast("int").alias("area"),
        F.col("SITUACAO").cast("int").alias("operating_status"),
        ct.to_coordinate(F.col("LATITUDE")).alias("latitude"),
        ct.to_coordinate(F.col("LONGITUDE")).alias("longitude"),
        F.col("QT_SALAS").cast("int").alias("classroom_count"),
        F.col("TEM_AGUA").cast("int").alias("has_water"),
        F.col("TEM_ENERGIA").cast("int").alias("has_electricity"),
        F.col("TEM_ESGOTO").cast("int").alias("has_sewage"),
        F.col("TEM_BANHEIRO").cast("int").alias("has_bathroom"),
        F.col("TEM_BIBLIOTECA").cast("int").alias("has_library"),
        F.col("TEM_LAB_INFO").cast("int").alias("has_computer_lab"),
        F.col("TEM_QUADRA").cast("int").alias("has_sports_court"),
        F.col("TEM_INTERNET").cast("int").alias("has_internet"),
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r09/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

# CSV (header, separador ";") com todas as colunas em texto; como no raw de
# escola, há uma coluna final "ano" à direita que o schema explícito ignora.
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
    """Lê o raw de matrículas por unidade, padroniza nomes e tipa as contagens."""
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        F.trim(F.col("ID_UNIDADE")).alias("id_unidade"),
        F.col("MAT_BASICA").cast("int").alias("enrollment_basic"),
        F.col("MAT_INFANTIL").cast("int").alias("enrollment_infant"),
        F.col("MAT_FUNDAMENTAL").cast("int").alias("enrollment_elementary"),
        F.col("MAT_MEDIO").cast("int").alias("enrollment_highschool"),
        F.col("MAT_PROFISSIONAL").cast("int").alias("enrollment_professional"),
        F.col("MAT_EJA").cast("int").alias("enrollment_youth_adult"),
        F.col("MAT_ESPECIAL").cast("int").alias("enrollment_special"),
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r09/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_matricula", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")


def main():
    """Grade H3 (nível 5) das escolas federais em atividade.

    Para cada célula H3 calcula: nº de escolas, total e média de matrículas, o nº
    de escolas federais vizinhas (mesma célula + células adjacentes, exceto a
    própria — igual para toda a célula), a distância geodésica média de cada
    escola às suas vizinhas e a distância geodésica média de cada escola à federal
    mais próxima (qualquer, sem restrição de célula). Distâncias medidas com
    ``ST_DistanceSpheroid`` sobre os pontos WGS84 originais.
    """
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schools = spark.read.format("geoparquet").load(ORIGEM)

    # Ponto WGS84 (para a distância geodésica) e disco H3 k=1 da célula da escola
    # (célula própria + 6 adjacentes; ``exact_ring=False`` inclui o centro).
    prepared = schools.select(
        F.col("id_unidade"),
        F.col("h3_cell"),
        F.col("enrollment"),
        stc.ST_Point(F.col("longitude"), F.col("latitude")).alias("point_wgs84"),
        stf.ST_H3KRing(F.col("h3_cell"), F.lit(1), F.lit(False)).alias("h3_disk"),
    )

    # Base por célula: contagem e matrículas independem do cálculo de vizinhança.
    cell_base = prepared.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("enrollment").cast("long").alias("total_enrollment"),
    )

    # Pares ordenados de escolas distintas. O conjunto é pequeno (centenas de
    # escolas federais), então o produto cartesiano é barato. Uma escola ``b`` é
    # vizinha de ``a`` quando a célula de ``b`` está no disco H3 da célula de ``a``.
    left = prepared.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("point_wgs84").alias("a_point"),
        F.col("h3_disk").alias("a_disk"),
    )
    right = prepared.select(
        F.col("id_unidade").alias("b_id"),
        F.col("h3_cell").alias("b_cell"),
        F.col("point_wgs84").alias("b_point"),
    )
    pairs = (
        left.crossJoin(right)
        .filter(F.col("a_id") != F.col("b_id"))
        .withColumn(
            "dist_m", stf.ST_DistanceSpheroid(F.col("a_point"), F.col("b_point"))
        )
        .withColumn("is_neighbor", F.array_contains(F.col("a_disk"), F.col("b_cell")))
    )

    # Por escola: média das distâncias às vizinhas, nº de vizinhas (= N-1, constante
    # na célula) e distância à federal mais próxima (mínimo sobre todas as demais).
    per_school = pairs.groupBy("a_id", "a_cell").agg(
        F.avg(F.when(F.col("is_neighbor"), F.col("dist_m"))).alias("mean_dist_neighbors_m"),
        F.sum(F.when(F.col("is_neighbor"), F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("n_neighbors"),
        F.min("dist_m").alias("nearest_m"),
    )

    # Por célula: o nº de vizinhas é o mesmo para toda a célula (usa ``max``, que
    # coincide com qualquer escola); as distâncias são promediadas entre as escolas.
    cell_neighbor = per_school.groupBy("a_cell").agg(
        F.max("n_neighbors").alias("n_federal_neighbors"),
        F.avg("mean_dist_neighbors_m").alias("mean_dist_neighbors_m"),
        F.avg("nearest_m").alias("mean_dist_nearest_m"),
    ).withColumnRenamed("a_cell", "h3_cell")

    grid = (
        dt.join_no_fanout(cell_base, cell_neighbor, on="h3_cell", how="left")
        .withColumn(
            "avg_enrollment",
            F.round(F.col("total_enrollment") / F.col("n_schools"), 2),
        )
        .withColumn(
            "avg_dist_neighbors_km",
            F.round(F.col("mean_dist_neighbors_m") / F.lit(1000.0), 3),
        )
        .withColumn(
            "avg_dist_nearest_km",
            F.round(F.col("mean_dist_nearest_m") / F.lit(1000.0), 3),
        )
        # Polígono da célula (ST_H3ToGeom devolve em EPSG:4326) reprojetado para
        # EPSG:3857, o CRS de saída do lake.
        .withColumn(
            "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
        )
        .transform(dt.to_web_mercator)
    )

    result = grid.select(
        "h3_cell",
        "n_schools",
        "total_enrollment",
        "avg_enrollment",
        "n_federal_neighbors",
        "avg_dist_neighbors_km",
        "avg_dist_nearest_km",
        "geometry",
    )

    result.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r09/workdir/apps/educacao/jobs/silver/educacao_escola_matricula.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from apps.educacao.utils import column_transforms as edu_ct
from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula", "silver")

H3_LEVEL = 5


def main():
    """Escolas federais em atividade, georreferenciadas e com matrículas anexadas.

    Restringe às escolas da rede federal (``SETOR = 1``) em atividade
    (``SITUACAO = 1``) com coordenadas válidas dentro do Brasil, anexa o total de
    matrículas da educação básica e indexa cada escola na célula H3 (nível 5).
    Preserva latitude/longitude WGS84 para as medidas geodésicas do gold.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA)

    federal = (
        escola.filter(
            edu_ct.is_federal(F.col("admin_sector"))
            & edu_ct.is_in_activity(F.col("operating_status"))
        )
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        # Ponto em WGS84 e célula H3 (nível 5) calculada em coordenadas geográficas.
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(lambda d: dt.attach_h3_index(d, "geometry", H3_LEVEL, "h3_cell"))
    )

    # Matrícula é única por unidade (chave ID_UNIDADE); left join preserva as
    # escolas sem registro de matrícula, sem risco de fan-out.
    enrollment = matricula.select(
        F.col("id_unidade"),
        F.col("enrollment_basic").cast("long").alias("enrollment"),
    )
    joined = dt.join_no_fanout(federal, enrollment, on="id_unidade", how="left")

    silver = (
        joined.transform(dt.to_web_mercator)  # geometria persistida em EPSG:3857
        .select(
            "id_unidade",
            "school_name",
            "uf",
            "municipality_name",
            "admin_sector",
            "operating_status",
            "enrollment",
            "latitude",
            "longitude",
            "h3_cell",
            "geometry",
        )
    )

    silver.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_matricula: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

