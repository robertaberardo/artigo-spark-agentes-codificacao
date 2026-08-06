# === /d/runs/C5-final-20260801-103603-r01/workdir/apps/educacao/jobs/bronze/educacao_escola.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola", "raw")
DESTINO = table_location("educacao", "escola", "bronze")

# Schema do raw em ordem posicional (todo texto). Com header + schema, o Spark
# ignora os nomes do cabeçalho e aplica estes nomes por posição.
RAW_SCHEMA = StructType(
    [
        StructField("id_unidade", StringType()),
        StructField("nome_unidade", StringType()),
        StructField("uf", StringType()),
        StructField("cod_uf", StringType()),
        StructField("nome_municipio", StringType()),
        StructField("cod_municipio", StringType()),
        StructField("setor", StringType()),
        StructField("area", StringType()),
        StructField("situacao", StringType()),
        StructField("latitude", StringType()),
        StructField("longitude", StringType()),
        StructField("qt_salas", StringType()),
        StructField("tem_agua", StringType()),
        StructField("tem_energia", StringType()),
        StructField("tem_esgoto", StringType()),
        StructField("tem_banheiro", StringType()),
        StructField("tem_biblioteca", StringType()),
        StructField("tem_lab_info", StringType()),
        StructField("tem_quadra", StringType()),
        StructField("tem_internet", StringType()),
    ]
)


def main():
    """Lê o raw das escolas, padroniza nomes/tipos e salva a bronze."""
    spark = SparkSession.builder.appName("educacao-escola-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(raw["id_unidade"], 8).alias("id_unidade"),
        ct.normalize_text(raw["nome_unidade"]).alias("nome_unidade"),
        ct.standardize_uf(raw["uf"]).alias("uf"),
        ct.digits_only(raw["cod_municipio"]).alias("cod_municipio"),
        raw["setor"].cast("int").alias("setor"),
        raw["situacao"].cast("int").alias("situacao"),
        ct.to_coordinate(raw["latitude"]).alias("latitude"),
        ct.to_coordinate(raw["longitude"]).alias("longitude"),
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_escola: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-103603-r01/workdir/apps/educacao/jobs/bronze/educacao_matricula.py ===
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from utils import column_transforms as ct
from utils.metadata import table_location

ORIGEM = table_location("educacao", "matricula", "raw")
DESTINO = table_location("educacao", "matricula", "bronze")

# Schema do raw em ordem posicional (todo texto). Só a chave e o total da
# educação básica seguem para a bronze; os demais recortes não são usados aqui.
RAW_SCHEMA = StructType(
    [
        StructField("id_unidade", StringType()),
        StructField("mat_basica", StringType()),
        StructField("mat_infantil", StringType()),
        StructField("mat_fundamental", StringType()),
        StructField("mat_medio", StringType()),
        StructField("mat_profissional", StringType()),
        StructField("mat_eja", StringType()),
        StructField("mat_especial", StringType()),
    ]
)


def main():
    """Lê o raw das matrículas, padroniza chave/total e salva a bronze."""
    spark = SparkSession.builder.appName("educacao-matricula-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .schema(RAW_SCHEMA)
        .csv(ORIGEM)
    )

    bronze = raw.select(
        ct.zero_pad_code(raw["id_unidade"], 8).alias("id_unidade"),
        raw["mat_basica"].cast("int").alias("mat_basica"),
    ).filter(F.col("id_unidade").isNotNull())

    bronze.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> bronze educacao_matricula: {n} registros em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-103603-r01/workdir/apps/educacao/jobs/gold/educacao_escola_matricula_h3_grid.py ===
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("educacao", "escola_geo", "silver")
DESTINO = table_location("educacao", "escola_matricula_h3_grid", "gold")

# São vizinhas as escolas na própria célula ou nas imediatamente adjacentes
# (anel H3 de raio 1). As escolas já vêm indexadas em H3 nível 5 do silver.
NEIGHBOR_RING = 1


def main():
    """Agregação por célula H3 nível 5 das escolas federais em atividade."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-h3-grid-gold").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.format("geoparquet").load(ORIGEM)

    # Distâncias geodésicas exigem o ponto em WGS84 (lat/long original), medido
    # antes de qualquer projeção — reconstrói o ponto a partir de lat/long.
    with_metrics = (
        escola.transform(
            lambda d: dt.add_point_geometry(d, "latitude", "longitude", "point_wgs84")
        )
        .transform(
            lambda d: dt.add_nearest_other_distance_km(
                d, "id_unidade", "point_wgs84", "dist_nearest_km"
            )
        )
        .transform(
            lambda d: dt.add_h3_ring_neighbor_stats(
                d,
                "id_unidade",
                "h3_cell",
                "point_wgs84",
                k=NEIGHBOR_RING,
                count_col="n_neighbors",
                dist_col="dist_neighbors_km",
            )
        )
    )

    # n_federal_neighbors é constante na célula (o anel é o mesmo para todas as
    # escolas dela), então F.max apenas materializa esse valor comum.
    hexbin = with_metrics.transform(
        lambda d: dt.aggregate_by_h3(
            d,
            "h3_cell",
            [
                F.count(F.lit(1)).cast("long").alias("n_schools"),
                F.sum("enrollment").cast("long").alias("total_enrollment"),
                F.round(F.avg("enrollment"), 1).alias("avg_enrollment"),
                F.max("n_neighbors").cast("long").alias("n_federal_neighbors"),
                F.round(F.avg("dist_neighbors_km"), 3).alias("avg_dist_neighbors_km"),
                F.round(F.avg("dist_nearest_km"), 3).alias("avg_dist_nearest_km"),
            ],
        )
    ).transform(dt.to_web_mercator).select(
        "h3_cell",
        "n_schools",
        "total_enrollment",
        "avg_enrollment",
        "n_federal_neighbors",
        "avg_dist_neighbors_km",
        "avg_dist_nearest_km",
        "geometry",
    )

    hexbin.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> gold educacao_escola_matricula_h3_grid: {n} células em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

# === /d/runs/C5-final-20260801-103603-r01/workdir/apps/educacao/jobs/silver/educacao_escola_geo.py ===
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ESCOLA = table_location("educacao", "escola", "bronze")
MATRICULA = table_location("educacao", "matricula", "bronze")
DESTINO = table_location("educacao", "escola_geo", "silver")

SETOR_FEDERAL = 1
SITUACAO_ATIVA = 1
H3_LEVEL = 5


def filter_federal_ativa(df: DataFrame) -> DataFrame:
    """Mantém apenas escolas do setor federal e em atividade."""
    return df.filter(
        (F.col("setor") == SETOR_FEDERAL) & (F.col("situacao") == SITUACAO_ATIVA)
    )


def main():
    """Escolas federais em atividade georreferenciadas: ponto, célula H3 e matrículas."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("educacao-escola-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    escola = spark.read.parquet(ESCOLA)
    matricula = spark.read.parquet(MATRICULA)

    geo = (
        escola.transform(filter_federal_ativa)
        .transform(lambda d: dt.filter_valid_coordinates(d, "latitude", "longitude"))
        # left join: preserva escolas sem registro de matrícula (ausência ≠ zero)
        .transform(lambda d: dt.join_no_fanout(d, matricula, "id_unidade", how="left"))
        .withColumnRenamed("mat_basica", "enrollment")
        # célula H3 é calculada sobre o ponto em WGS84, antes de projetar
        .transform(lambda d: dt.add_point_geometry(d, "latitude", "longitude"))
        .transform(lambda d: dt.attach_h3_index(d, "geometry", H3_LEVEL, "h3_cell"))
        .transform(dt.to_web_mercator)
        .select(
            "id_unidade",
            "nome_unidade",
            "uf",
            "latitude",
            "longitude",
            "enrollment",
            "h3_cell",
            "geometry",
        )
    )

    geo.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver educacao_escola_geo: {n} escolas em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()

