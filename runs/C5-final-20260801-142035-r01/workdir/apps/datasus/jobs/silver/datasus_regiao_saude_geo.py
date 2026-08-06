from pyspark.sql import functions as F
from sedona.spark import SedonaContext
from sedona.spark.sql import st_functions as stf

from utils import dataframe_transforms as dt
from utils.metadata import table_location

ORIGEM = table_location("datasus", "regiao_saude", "bronze")
DESTINO = table_location("datasus", "regiao_saude_geo", "silver")


def main():
    """Regiões de saúde georreferenciadas: polígono, área em km² e geometria 3857."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("datasus-regiao-saude-geo-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    regiao = spark.read.parquet(ORIGEM)

    geo = dt.add_line_geometry(regiao, "geom_wkt")
    geo = dt.to_web_mercator(geo)

    geo = geo.withColumn(
        "area_km2", stf.ST_Area(F.col("geometry")) / F.lit(1_000_000.0)
    )

    result = geo.select("co_regiao", "nome", "uf", "area_km2", "geometry")

    result.write.mode("overwrite").format("geoparquet").save(DESTINO)
    n = spark.read.format("geoparquet").load(DESTINO).count()
    print(f">> silver datasus_regiao_saude_geo: {n} regiões em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
