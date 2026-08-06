# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Idioma

Este repositório é bilíngue por convenção (ver `CODING_STANDARDS.md`): **código, nomes e colunas em inglês; docstrings, comentários e mensagens de log em português.** Siga o mesmo padrão ao escrever ou editar código aqui.

## O que é este projeto

Pipelines geoespaciais PySpark + Apache Sedona sobre dados abertos brasileiros, num data lake S3A (MinIO). Cada fonte de dado é uma **app** em `apps/<app>/` (ana, anac, bcb, caged, cvm, datasus, dnit, educacao, ibge, inmet, snis, tse). Tudo roda dentro de um container Docker — não há Spark/GDAL instalados no host.

## Arquitetura medalhão

Os jobs de cada app estão organizados por camada, e essa é a estrutura mais importante para entender o fluxo:

```
apps/<app>/jobs/bronze/<tabela>.py   raw (CSV) → bronze: schema explícito, tipagem, limpeza de coluna
apps/<app>/jobs/silver/<tabela>.py   bronze → silver: joins, geometria (Sedona), consolidação
apps/<app>/jobs/gold/<tabela>.py     silver → gold: agregações, rankings, hexbins H3
```

- **Cada job é executável independente** com `def main()` e persiste seu(s) DataFrame(s) no fim. Não há orquestrador; jobs rodam um a um via `spark-submit`. A ordem de dependência é raw→bronze→silver→gold dentro de uma mesma tabela/app.
- **bronze** grava `.parquet`; **silver/gold com geometria** gravam `.format("geoparquet")`. Sempre releia e conte (`n = spark.read...count()`) após gravar — é o padrão de verificação usado em todos os jobs.
- Duas formas de criar a sessão, conforme o job usa Sedona ou não:
  - Sem geoespacial: `SparkSession.builder.appName(...).getOrCreate()`
  - Com geoespacial: `SedonaContext.create(SedonaContext.builder().appName(...).getOrCreate())` — obrigatório para qualquer função `ST_*`.

## `metadata.toml` é a fonte da verdade

Cada app tem um `apps/<app>/metadata.toml` que declara **toda tabela, suas camadas (`levels`), a chave e o schema por camada**. Os jobs **não montam caminhos S3A à mão**: usam `utils.metadata.table_location(app, table, level)`, que resolve `s3a://{bucket}/{app}/{name}/{level}` (bucket via env `S3_BUCKET`). Ao criar uma tabela ou camada nova, **declare-a primeiro no `metadata.toml`** (incluindo o schema da camada) e leia os caminhos por `table_location`. `table_location` levanta `KeyError`/`ValueError` de propósito quando a tabela ou a camada não existe — falha cedo em vez de ler um prefixo vazio.

## Reúso de transformações — leia antes de escrever lógica nova

Antes de escrever qualquer transformação, procure se ela já existe. A regra do repositório é: transformações genéricas pertencem a `utils/`, não ao corpo do job.

- `utils/column_transforms.py` — funções `Column -> Column` (parsing BR de número/data/moeda, limpeza de texto, nulos, arrays/maps, geometria Sedona, chaves).
- `utils/dataframe_transforms.py` — funções `DataFrame -> DataFrame` (coordenadas/pontos, distância geodésica, reprojeção, grades H3, janelas/ranking, joins sem fan-out, flatten JSON).
- `apps/<app>/utils/column_transforms.py` — transformações **específicas do domínio** daquela app (ex.: `is_valid_station_code` na ana). Só o que é específico da fonte fica aqui.

Encadeie via `DataFrame.transform(...)` em vez de reatribuir a cada passo. Import padrão: `from pyspark.sql import types as T, functions as F`.

## Convenções que mais afetam o código (detalhes em `CODING_STANDARDS.md`)

`CODING_STANDARDS.md` é o guia de estilo (baseado no Palantir PySpark Style Guide) e vale para todo código. Os pontos de maior impacto:

- **Nunca UDFs.** Reescreva com funções nativas do PySpark. `replace_multiples` mostra como fazer substituição em massa sem UDF.
- **Prefira `F.funcao_nativa(...)` a `F.expr("...")`**; só use `F.expr` quando não houver equivalente nativa, e comente o porquê.
- **Ausência é `F.lit(None)`, nunca `""`/`"NA"`/`0`.** Não converta nulos em zero antes de agregar.
- **Schema explícito**, nunca `inferSchema`; **latitude/longitude sempre `double`**.
- **Distância geodésica geo:** meça em metros com `ST_DistanceSpheroid` sobre o ponto **WGS84 original, ANTES de projetar** para 3857. Não faça round-trip 4326→3857→4326 só para medir. Ver `apps/ana/jobs/silver/ana_estacao_geo.py` como referência canônica.
- **Joins:** `how` sempre explícito; evite `right`; cuidado com fan-out. `dataframe_transforms.join_no_fanout` valida unicidade da chave em vez de mascarar duplicatas com `dropDuplicates`/`distinct`.
- **Janelas:** sempre frame explícito (`rowsBetween`/`rangeBetween`); evite `partitionBy()` vazio.

## Comandos

Tudo roda no container `spark` (Fedora 39 + Java 11 + Python 3.11 + GDAL + jars de Sedona/S3A pré-baixados). O projeto é montado em `/workspace/spark-project` e é `PYTHONPATH`, então `from utils... import` e `from apps.<app>... import` funcionam a partir da raiz.

```bash
# Subir o ambiente (requer a rede docker externa `external-net`)
docker compose up -d --build

# Abrir shell no container
docker compose exec spark bash

# Rodar um job (a partir da raiz do projeto, dentro do container)
spark-submit apps/ana/jobs/bronze/ana_estacao.py
spark-submit apps/ana/jobs/silver/ana_estacao_geo.py

# Spark UI: http://localhost:14040   |   Jupyter: http://localhost:18888
```

Não há suíte de testes automatizados nem lint configurado no repositório. A verificação, conforme `CODING_STANDARDS.md`, é **rodar o job e conferir a contagem/o dado de saída** (todo job já imprime `n` registros gravados). As funções de `utils/` são puras e testáveis isoladamente — prefira validá-las contra um DataFrame pequeno.

## Notas de ambiente

- Credenciais/endpoints S3A vêm de env (`.env`, exemplo em `.env.example`); a config S3A do Spark está em `conf/spark-defaults.conf` (lido via `SPARK_CONF_DIR`).
- O `Dockerfile` monta as camadas na ordem GDAL → deps Python → jars justamente para manter o compile caro do GDAL em cache; editar `pyproject.toml` não reconstrói o GDAL.
- É um ambiente de **validação**: o `docker/resources/start.sh` cita `validate_setup.py` e `exploration/` que podem não existir, e alguns jobs podem divergir das convenções acima de propósito — trate `CODING_STANDARDS.md` e `metadata.toml` como o alvo correto.
</content>
</invoke>
