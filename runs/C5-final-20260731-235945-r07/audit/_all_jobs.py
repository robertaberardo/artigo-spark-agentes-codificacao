# === /d/runs/C5-final-20260731-235945-r07/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from apps.educacao.utils import column_transforms as educacao_ct
from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

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
    """Lê o raw das escolas, tipa/limpa as colunas e salva a bronze.

    Coordenadas viram ``double``, os indicadores 0/1 viram ``int`` e as linhas com
    ``ID_UNIDADE`` malformado são descartadas.
    """
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
        ct.normalize_text(raw["NOME_UNIDADE"]).alias("name"),
        ct.standardize_uf(raw["UF"]).alias("uf"),
        ct.blank_to_null(raw["COD_UF"]).alias("cod_uf"),
        ct.normalize_text(raw["NOME_MUNICIPIO"]).alias("name_municipio"),
        ct.blank_to_null(raw["COD_MUNICIPIO"]).alias("cod_municipio"),
        ct.blank_to_null(raw["SETOR"]).alias("setor"),
        ct.blank_to_null(raw["AREA"]).alias("area"),
        ct.blank_to_null(raw["SITUACAO"]).alias("situacao"),
        ct.to_coordinate(raw["LATITUDE"]).alias("latitude"),
        ct.to_coordinate(raw["LONGITUDE"]).alias("longitude"),
        ct.blank_to_null(raw["QT_SALAS"]).cast("int").alias("qt_salas"),
        ct.yes_no_to_int(raw["TEM_AGUA"]).alias("has_water"),
        ct.yes_no_to_int(raw["TEM_ENERGIA"]).alias("has_power"),
        ct.yes_no_to_int(raw["TEM_ESGOTO"]).alias("has_sewage"),
        ct.yes_no_to_int(raw["TEM_BANHEIRO"]).alias("has_bathroom"),
        ct.yes_no_to_int(raw["TEM_BIBLIOTECA"]).alias("has_library"),
        ct.yes_no_to_int(raw["TEM_LAB_INFO"]).alias("has_computer_lab"),
        ct.yes_no_to_int(raw["TEM_QUADRA"]).alias("has_sports_court"),
        ct.yes_no_to_int(raw["TEM_INTERNET"]).alias("has_internet"),
    ).filter(educacao_ct.is_valid_id_unidade(raw["ID_UNIDADE"]))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r07/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from apps.educacao.utils import column_transforms as educacao_ct
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
    """Lê o raw das matrículas, tipa as contagens como ``int`` e salva a bronze.

    As linhas com ``ID_UNIDADE`` malformado são descartadas para casar com a chave
    das escolas.
    """
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(raw["ID_UNIDADE"], 8).alias("id_unidade"),
        ct.blank_to_null(raw["MAT_BASICA"]).cast("int").alias("enrollment_basic"),
        ct.blank_to_null(raw["MAT_INFANTIL"]).cast("int").alias("enrollment_infant"),
        ct.blank_to_null(raw["MAT_FUNDAMENTAL"]).cast("int").alias("enrollment_elementary"),
        ct.blank_to_null(raw["MAT_MEDIO"]).cast("int").alias("enrollment_highschool"),
        ct.blank_to_null(raw["MAT_PROFISSIONAL"]).cast("int").alias("enrollment_professional"),
        ct.blank_to_null(raw["MAT_EJA"]).cast("int").alias("enrollment_youth_adult"),
        ct.blank_to_null(raw["MAT_ESPECIAL"]).cast("int").alias("enrollment_special"),
    ).filter(educacao_ct.is_valid_id_unidade(raw["ID_UNIDADE"]))

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260731-235945-r07/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
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
SETOR_FEDERAL = "1"
SITUACAO_ATIVA = "1"


def federal_active_schools(df: DataFrame) -> DataFrame:
    """Escolas federais em atividade, com ponto WGS84 e célula H3 nível 5.

    Filtra setor federal e situação em atividade, reconstrói o ponto original em
    EPSG:4326 a partir de ``latitude``/``longitude`` (para medir distância
    geodésica sem passar pelo 3857) e anexa a célula H3 (nível 5) que o contém.
    """
    ponto_4326 = stc.ST_Point(F.col("longitude"), F.col("latitude"))
    return (
        df.filter(
            (F.col("setor") == SETOR_FEDERAL) & (F.col("situacao") == SITUACAO_ATIVA)
        )
        .withColumn("geom_4326", ponto_4326)
        .transform(lambda d: dt.attach_h3_index(d, "geom_4326", H3_LEVEL, "h3_cell"))
        .select("id_unidade", "h3_cell", "total_enrollment", "geom_4326")
    )


def cell_neighbor_counts(schools: DataFrame) -> DataFrame:
    """Nº de escolas federais vizinhas por célula (mesmo valor para toda a célula).

    A vizinhança de uma célula é ela própria mais as células imediatamente
    adjacentes (``ST_H3KRing`` com k=1). O total de escolas nessa vizinhança é
    ``N``; como uma escola não é vizinha de si mesma, cada escola da célula tem
    ``N - 1`` vizinhas — número idêntico para todas as escolas da célula.
    """
    cell_counts = schools.groupBy("h3_cell").agg(F.count(F.lit(1)).alias("cnt"))
    neighborhood = cell_counts.select(
        "h3_cell",
        F.explode(stf.ST_H3KRing(F.col("h3_cell"), F.lit(1), F.lit(False))).alias(
            "neighbor_cell"
        ),
    )
    lookup = cell_counts.select(
        F.col("h3_cell").alias("neighbor_cell"), F.col("cnt").alias("neighbor_cnt")
    )
    return (
        neighborhood.join(lookup, on="neighbor_cell", how="left")
        .groupBy("h3_cell")
        .agg(
            (F.sum(F.coalesce("neighbor_cnt", F.lit(0))) - F.lit(1))
            .cast("long")
            .alias("n_federal_neighbors")
        )
    )


def cell_neighbor_distance(schools: DataFrame) -> DataFrame:
    """Distância média de cada escola às suas vizinhas, agregada por célula (km).

    Para cada escola A, as vizinhas são as demais escolas B situadas na célula de
    A ou nas adjacentes (``ST_H3KRing`` k=1). Mede a distância geodésica A–B
    (``ST_DistanceSpheroid`` sobre o ponto WGS84), tira a média por escola e, em
    seguida, a média dessas médias por célula. Escolas sem vizinha não entram.
    """
    a = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("geom_4326").alias("a_geom"),
    ).withColumn(
        "neighbor_cell",
        F.explode(stf.ST_H3KRing(F.col("a_cell"), F.lit(1), F.lit(False))),
    )
    b = schools.select(
        F.col("id_unidade").alias("b_id"),
        F.col("h3_cell").alias("neighbor_cell"),
        F.col("geom_4326").alias("b_geom"),
    )
    pairs = (
        a.join(b, on="neighbor_cell", how="inner")
        .filter(F.col("a_id") != F.col("b_id"))
        .withColumn("dist_m", stf.ST_DistanceSpheroid(F.col("a_geom"), F.col("b_geom")))
    )
    per_school = pairs.groupBy("a_cell", "a_id").agg(
        F.avg("dist_m").alias("avg_dist_school_m")
    )
    return per_school.groupBy("a_cell").agg(
        F.round(F.avg("avg_dist_school_m") / F.lit(1000.0), 3).alias(
            "avg_dist_neighbors_km"
        )
    ).withColumnRenamed("a_cell", "h3_cell")


def cell_nearest_distance(schools: DataFrame) -> DataFrame:
    """Distância média de cada escola à federal mais próxima (qualquer), por célula.

    Sem restrição de célula: para cada escola A, busca a menor distância geodésica
    a qualquer outra escola federal em atividade (produto cartesiano; o conjunto de
    federais é pequeno e vai em ``broadcast``), depois tira a média por célula (km).
    """
    a = schools.select(
        F.col("id_unidade").alias("a_id"),
        F.col("h3_cell").alias("a_cell"),
        F.col("geom_4326").alias("a_geom"),
    )
    b = schools.select(
        F.col("id_unidade").alias("b_id"),
        F.col("geom_4326").alias("b_geom"),
    )
    nearest_per_school = (
        a.crossJoin(F.broadcast(b))
        .filter(F.col("a_id") != F.col("b_id"))
        .withColumn("dist_m", stf.ST_DistanceSpheroid(F.col("a_geom"), F.col("b_geom")))
        .groupBy("a_cell", "a_id")
        .agg(F.min("dist_m").alias("nearest_m"))
    )
    return nearest_per_school.groupBy("a_cell").agg(
        F.round(F.avg("nearest_m") / F.lit(1000.0), 3).alias("avg_dist_nearest_km")
    ).withColumnRenamed("a_cell", "h3_cell")


def main():
    """Grade H3 nível 5 das escolas federais em atividade.

    Para cada célula: nº de escolas, total e média de matrículas, nº de vizinhas
    federais, distância média às vizinhas e distância média à federal mais próxima.
    A geometria de saída é o polígono da célula em EPSG:3857.
    """
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("educacao-escola-matricula-h3-grid-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("geoparquet").load(ORIGEM)
    schools = federal_active_schools(silver).cache()

    base = schools.groupBy("h3_cell").agg(
        F.count(F.lit(1)).cast("long").alias("n_schools"),
        F.sum("total_enrollment").cast("long").alias("total_enrollment"),
        F.round(F.avg("total_enrollment"), 2).alias("avg_enrollment"),
    )

    grid = (
        base.join(cell_neighbor_counts(schools), on="h3_cell", how="left")
        .join(cell_neighbor_distance(schools), on="h3_cell", how="left")
        .join(cell_nearest_distance(schools), on="h3_cell", how="left")
        .withColumn(
            "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col("h3_cell"))), 1)
        )
        .transform(lambda d: dt.to_web_mercator(d, "geometry"))
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

# === /d/runs/C5-final-20260731-235945-r07/workdir/apps/educacao/jobs/silver/educacao_escola_matricula.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_matricula", "silver")


def main():
    """Consolida escolas + matrículas e georreferencia como ponto (EPSG:3857).

    Junta a bronze de escolas com a de matrículas pela unidade (``left`` — uma
    escola sem matrícula fica com ``total_enrollment`` nulo, sem preenchimento
    artificial), descarta coordenadas ausentes/fora do Brasil e cria o ponto em
    3857. ``latitude``/``longitude`` (WGS84) são preservadas para permitir medir
    distâncias geodésicas na gold sobre o ponto original, sem passar pelo 3857.
    """
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-matricula-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA).select(
        "id_unidade",
        "name",
        "uf",
        "cod_municipio",
        "setor",
        "situacao",
        "area",
        "latitude",
        "longitude",
    )
    matricula = spark.read.parquet(MATRICULA).select(
        "id_unidade",
        F.col("enrollment_basic").alias("total_enrollment"),
    )

    silver = (
        dt.join_no_fanout(escola, matricula, "id_unidade", "left")
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude", "geometry"))
        .transform(lambda d: dt.to_web_mercator(d, "geometry"))
        .select(
            "id_unidade",
            "name",
            "uf",
            "cod_municipio",
            "setor",
            "situacao",
            "area",
            "total_enrollment",
            "latitude",
            "longitude",
            "geometry",
        )
    )

    silver.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_matricula: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

