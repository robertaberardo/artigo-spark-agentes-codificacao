"""Profiling das tabelas do Censo Escolar 2025 lidas do MinIO.

Gera um relatório conciso para fundamentar as funções de limpeza (Fase 3):
dimensões, duplicatas de chave, validade de lat/long, taxa de nulos e
integridade dos joins entre as tabelas (via CO_ENTIDADE).

Execução (dentro do container):
    spark-submit $PROJECT_PATH/../evaluation/data/profile.py
"""
import os

from pyspark.sql import functions as F
from sedona.spark import SedonaContext

BUCKET = os.environ.get("S3_BUCKET", "datalake")
ANO = "2025"
# Convenção do data lake: s3://<bucket>/inep/inep_<tabela>/raw/ano=<ANO>/<arquivo>.csv
BASE = f"s3a://{BUCKET}/inep"

TABELAS = {
    "escola": "Tabela_Escola_2025.csv",
    "matricula": "Tabela_Matricula_2025.csv",
    "turma": "Tabela_Turma_2025.csv",
    "docente": "Tabela_Docente_2025.csv",
    "gestor": "Tabela_Gestor_Escolar_2025.csv",
    "curso_tecnico": "Tabela_Curso_Tecnico_2025.csv",
}


def ler(spark, tabela, arquivo):
    """Lê um CSV do Inep (latin-1, separado por ponto e vírgula, com cabeçalho)."""
    caminho = f"{BASE}/inep_{tabela}/raw/ano={ANO}/{arquivo}"
    return (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("encoding", "ISO-8859-1")
        .csv(caminho)
    )


def main():
    spark = SedonaContext.create(
        SedonaContext.builder().appName("profile-censo").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    dfs = {nome: ler(spark, nome, arq) for nome, arq in TABELAS.items()}

    print("\n=== dimensoes ===")
    tamanhos = {}
    for nome, df in dfs.items():
        n = df.count()
        tamanhos[nome] = n
        print(f"{nome:14s} linhas={n:>12,d} colunas={len(df.columns)}")

    esc = dfs["escola"]
    total = tamanhos["escola"]

    print("\n=== escola: duplicatas de CO_ENTIDADE ===")
    distintos = esc.select("CO_ENTIDADE").distinct().count()
    print(f"linhas={total:,d} distintos={distintos:,d} duplicatas={total - distintos:,d}")

    print("\n=== escola: validade de lat/long ===")
    esc2 = esc.withColumn("lat", F.col("LATITUDE").cast("double")).withColumn(
        "lon", F.col("LONGITUDE").cast("double")
    )
    com_coords = esc2.filter(F.col("lat").isNotNull() & F.col("lon").isNotNull()).count()
    dentro = esc2.filter(
        F.col("lat").between(-34.0, 6.0) & F.col("lon").between(-74.0, -33.0)
    ).count()
    print(
        f"com_coords={com_coords:,d} ({100 * com_coords / total:.1f}%) "
        f"dentro_do_brasil={dentro:,d} ({100 * dentro / total:.1f}%)"
    )

    print("\n=== escola: top 15 colunas por taxa de nulos/vazios ===")
    exprs = [
        F.avg(
            F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1.0).otherwise(0.0)
        ).alias(c)
        for c in esc.columns
    ]
    taxas = esc.select(exprs).collect()[0].asDict()
    for c, v in sorted(taxas.items(), key=lambda kv: kv[1], reverse=True)[:15]:
        print(f"  {c:32s} {100 * v:5.1f}% nulos")

    print("\n=== integridade de join (CO_ENTIDADE existe em escola?) ===")
    chaves = esc.select("CO_ENTIDADE").distinct()
    for nome in ["matricula", "turma", "docente", "gestor"]:
        d = dfs[nome]
        if "CO_ENTIDADE" in d.columns:
            ref = d.select("CO_ENTIDADE").distinct()
            orfaos = ref.join(chaves, "CO_ENTIDADE", "left_anti").count()
            print(f"  {nome:12s} distintos={ref.count():>10,d} sem_correspondencia={orfaos:,d}")
        else:
            print(f"  {nome:12s} (sem coluna CO_ENTIDADE)")

    spark.stop()


if __name__ == "__main__":
    main()
