from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils.metadata import table_location

ORIGEM = table_location("ibge", "relation_municipio_populacao", "silver")
DESTINO = table_location("ibge", "relation_municipio_populacao_por_uf", "gold")


def main():
    """Lê o silver, agrega por UF e salva a gold."""
    spark = SedonaContext.create(
        SedonaContext.builder()
        .appName("ibge-relation-municipio-populacao-por-uf-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("geoparquet").load(ORIGEM)

    gold = (
        silver.groupBy("uf")
        .agg(
            F.count(F.lit(1)).alias("n_municipios"),
            F.sum("populacao").alias("populacao_total"),
            F.round(F.sum("area_km2"), 2).alias("area_km2_total"),
            F.round(F.avg("density"), 2).alias("density_media"),
            F.round(F.avg("distance_m"), 2).alias("distance_m_media"),
        )
        .orderBy(F.col("populacao_total").desc())
    )

    gold.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> gold ibge_relation_municipio_populacao_por_uf: {n} UFs em {DESTINO}")

    gold.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
