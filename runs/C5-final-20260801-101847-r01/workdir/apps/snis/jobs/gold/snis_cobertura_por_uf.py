from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils import column_transforms as ct

ORIGEM = "s3a://datalake/snis/snis_atendimento_municipal/silver"
DESTINO = "s3a://datalake/snis/snis_cobertura_por_uf/gold"


def main():
    """Médias estaduais de cobertura de água e esgoto, ponderadas pela população."""
    spark = SparkSession.builder.appName("snis-cobertura-por-uf-gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    atendimento = spark.read.parquet(ORIGEM)

    populacao = F.col("populacao")
    cobertura_agua = ct.safe_divide(
        F.sum(F.col("indice_atendimento_agua_pct") * populacao), F.sum(populacao)
    )
    cobertura_esgoto = ct.safe_divide(
        F.sum(F.col("indice_coleta_esgoto_pct") * populacao), F.sum(populacao)
    )

    gold = (
        atendimento.groupBy("uf")
        .agg(
            F.count(F.lit(1)).cast("long").alias("municipios"),
            F.sum(populacao).cast("long").alias("populacao_total"),
            F.round(cobertura_agua, 4).alias("cobertura_agua_media"),
            F.round(cobertura_esgoto, 4).alias("cobertura_esgoto_media"),
        )
        .orderBy(F.col("cobertura_agua_media").desc())
    )

    gold.write.mode("overwrite").parquet(DESTINO)
    n = spark.read.parquet(DESTINO).count()
    print(f">> gold snis_cobertura_por_uf: {n} UFs em {DESTINO}")
    gold.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
