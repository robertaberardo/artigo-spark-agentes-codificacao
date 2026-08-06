from pyspark.sql import functions as F
from sedona.spark import SedonaContext

from utils import column_transforms as ct
from utils import dataframe_transforms as dt

VOO = "s3a://datalake/anac/anac_voo/bronze"
EMPRESA = "s3a://datalake/anac/anac_empresa/bronze"
AERODROMO = "s3a://datalake/anac/anac_aerodromo/bronze"
DESTINO = "s3a://datalake/anac/anac_voo_consolidado/silver"


def main():
    """Voos com empresa e UF de origem resolvidas, e fator de aproveitamento."""
    spark = SedonaContext.create(
        SedonaContext.builder().appName("anac-voo-consolidado-silver").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    voo = spark.read.parquet(VOO)
    empresa = spark.read.parquet(EMPRESA).select(
        F.col("icao").alias("empresa_icao"), F.col("nome").alias("empresa_nome")
    )
    aerodromo = spark.read.parquet(AERODROMO).select(
        F.col("codigo_oaci").alias("origem_oaci"), F.col("uf").alias("origem_uf")
    )

    result = (
        voo.transform(lambda d: dt.join_lookup(d, empresa, "empresa_icao"))
        .transform(lambda d: dt.join_lookup(d, aerodromo, "origem_oaci"))
        .withColumn("passageiros", ct.coalesce_zero(F.col("passageiros")))
        .withColumn("assentos", ct.coalesce_zero(F.col("assentos")))
        .withColumn(
            "load_factor",
            ct.safe_divide(F.col("passageiros"), F.col("assentos")),
        )
        .select(
            "id_voo",
            "empresa_icao",
            "empresa_nome",
            "origem_oaci",
            "destino_oaci",
            "origem_uf",
            "data_partida",
            "passageiros",
            "assentos",
            "load_factor",
        )
    )

    result.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> silver anac_voo_consolidado: {n} voos em {DESTINO}")
    spark.stop()


if __name__ == "__main__":
    main()
