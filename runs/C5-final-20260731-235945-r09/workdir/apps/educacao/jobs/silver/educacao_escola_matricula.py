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
