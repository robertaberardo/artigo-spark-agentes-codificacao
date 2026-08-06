from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import dataframe_transforms as dt
from utils.metadata import table_location

MUNICIPIOS = table_location("ibge", "municipios", "bronze")
POPULACAO = table_location("ibge", "populacao", "bronze")
DESTINO = table_location("ibge", "relation_municipio_populacao", "silver")

BRASILIA_LAT, BRASILIA_LON = -15.7997, -47.8645


def main():
    """Junta municípios e população, enriquece com geometria/distância/densidade."""
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("ibge-relation-municipio-populacao-silver")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    municipios = spark.read.parquet(MUNICIPIOS)
    populacao = spark.read.parquet(POPULACAO)

    joined = dt.join_no_fanout(municipios, populacao, on="code", how="inner")

    result = (
        joined.transform(lambda d: dt.deduplicate_by_key(d, ["code"]))
        .transform(dt.filter_valid_coordinates)
        .transform(dt.add_point_geometry)
        .transform(
            lambda d: dt.add_distance_to_reference(d, BRASILIA_LAT, BRASILIA_LON)
        )
        .withColumn("density", F.col("populacao") / F.col("area_km2"))
    )

    result.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver ibge_relation_municipio_populacao: {n} municípios em {DESTINO}")

    result.drop("geometry").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
