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
