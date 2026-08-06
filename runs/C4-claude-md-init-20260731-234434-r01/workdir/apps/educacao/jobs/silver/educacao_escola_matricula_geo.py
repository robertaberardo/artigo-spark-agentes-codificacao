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
