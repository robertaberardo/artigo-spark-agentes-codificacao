# Padrões de código PySpark

Baseado no *Palantir PySpark Style Guide*, adaptado a este repositório. Vale
para todo código PySpark dos apps e das `utils/`.

## Idioma
- **Código, nomes de funções/variáveis e nomes de colunas: inglês.**
- **Docstrings e comentários: português.**

## Nomes de coluna
- **Dados transformados (bronze em diante): nomes de coluna em `minúsculo` (`snake_case`).**
- A leitura do dado cru referencia o nome **original** da fonte (mantém a caixa da origem);
  a padronização para minúsculo acontece já no `select` do bronze, via `.alias(...)`.

## Transformações como funções puras
- Modele cada passo como uma função `DataFrame -> DataFrame` (ou
  `Column -> Column`), sem efeito colateral e testável isoladamente.
- Encadeie com `DataFrame.transform(...)` em vez de reatribuir variáveis a
  cada passo.
- Antes de escrever uma transformação genérica, **verifique se ela já existe
  em `utils/`** (transformações de coluna e de DataFrame ficam em módulos
  separados) e reaproveite-a. Transformações genéricas novas pertencem a
  `utils/`, não ao corpo do job.
- **Não use UDFs.** São muito menos performáticas que as funções nativas. Quase
  toda lógica que parece exigir UDF pode ser reescrita com funções nativas do
  PySpark.

## Funções nativas da API python em vez de `F.expr`
- **Sempre prefira a função nativa `F.alguma_coisa(...)` a `F.expr("...")`.**
  Antes de recorrer a `F.expr`, procure a função equivalente em
  `pyspark.sql.functions`. A forma nativa é verificada em tempo de análise,
  evita erros de string SQL e mantém o código legível e testável.

```python
# ruim
df = df.withColumn("year", F.expr("year(event_date)"))
df = df.filter(F.expr("num > 0 and status = 'active'"))

# bom
df = df.withColumn("year", F.year("event_date"))
df = df.filter((F.col("num") > 0) & (F.col("status") == "active"))
```

- Só use `F.expr` quando de fato não existir função nativa equivalente; nesse
  caso, comente o **porquê**.

## Referências de coluna
- Refira colunas explicitamente com `F.col("nome")`, não por atributo
  (`df.nome`). A partir do Spark 3.0, muitas funções aceitam o nome da coluna
  como string simples (`F.lower("colA")`) — prefira essa forma quando possível.
- Referenciar por atributo do DataFrame (`df1.colA`) só é aceitável para
  **desambiguação** (ex.: em joins com nomes de coluna repetidos); nesses casos,
  prefira apelidar o DataFrame (`.alias(...)`) e referenciar `F.col("a.col")`.
- Evite `withColumnRenamed` encadeado; prefira um único `select` com `alias`.

```python
# ruim
df.select("key", "comments").withColumnRenamed("comments", "num_comments")

# bom
df.select("key", F.col("comments").alias("num_comments"))
```

- Evite `select("*")` seguido de `drop`; selecione explicitamente o que fica.
  Dropar colunas é aceitável depois de joins, onde é comum surgirem colunas
  redundantes.

## `select` como contrato de schema
- Um `select` no início e/ou antes do retorno da transformação declara o
  **contrato de schema** de entrada e saída. Trate todo `select` como uma
  operação de limpeza que prepara o DataFrame para o próximo passo.
- Mantenha o `select` simples: no máximo **uma** função de `spark.sql.functions`
  por coluna, mais um `.alias()` opcional. Se houver mais de três usos assim no
  mesmo `select`, extraia para uma função `clean_<nome>()`.
- Faça o `cast` de tipo dentro do `select`, não com `withColumn`:

```python
# ruim
df.select("comments").withColumn("comments", F.col("comments").cast("double"))

# bom
df.select(F.col("comments").cast("double"))
```

- Para adicionar **uma** coluna nova, use `withColumn`; para adicionar/manipular
  dezenas de colunas, use um único `select` (desempenho).
- Agrupe no `select` operações do mesmo tipo, quando a ordem não importar.

## Ausência de informação (nunca zero)
- Para preencher uma coluna vazia (satisfazer um schema), use **sempre**
  `F.lit(None)` — nunca string vazia nem `"NA"`. Assim preserva-se o uso de
  `isNull` e a semântica correta de ausência.

```python
# ruim
df = df.withColumn("foo", F.lit(""))

# bom
df = df.withColumn("foo", F.lit(None))
```

- **Ausência não é zero.** Em agregações, nulos **não** devem ser convertidos em
  `0` antes de agregar: `F.mean`/`F.avg`/`F.sum` já ignoram nulos, e transformá-los
  em `0` distorce médias e proporções. Só preencha ausência com `0` quando o
  **contrato** disser explicitamente que ausência conta como zero.

## Operações lógicas complexas
- Mantenha expressões lógicas (dentro de `.filter()` ou `F.when()`) com no
  máximo **três** operações por bloco. Extraia condições em variáveis nomeadas —
  fica mais legível, testável e reduz bugs (inclusive os difíceis de enxergar,
  como parênteses redundantes).

```python
# ruim
F.when((F.col("prod_status") == "Delivered") | (((F.datediff("deliveryDate_actual", "current_date") < 0) & ((F.col("currentRegistration") != "") | ...))), "In Service")

# bom
has_operator = (F.col("originalOperator") != "") | (F.col("currentOperator") != "")
delivery_date_passed = F.datediff("deliveryDate_actual", "current_date") < 0
has_registration = F.col("currentRegistration").rlike(".+")
is_delivered = F.col("prod_status") == "Delivered"
is_active = has_registration | has_operator

F.when(is_delivered | (delivery_date_passed & is_active), "In Service")
```

- Evite `.otherwise(value)` como fallback genérico: ao mapear chaves para
  valores, chaves inesperadas seriam todas mascaradas em um único valor.

## Leitura, schema e metadados
- Schema explícito em vez de `inferSchema`.
- Respeite os tipos do schema: quantidades numéricas como inteiros; **latitude e
  longitude sempre como `double`** (nunca string), para não perder precisão nem
  quebrar cálculos geoespaciais.
- A localização de cada tabela vem do `metadata.toml` da app — os jobs não
  montam caminhos à mão. Tabelas e camadas novas devem ser declaradas lá,
  incluindo o schema por camada.

## Joins
- Sempre explicite o `how`, mesmo quando for o padrão (`inner`).

- **Evite `right` join:** inverta a ordem dos DataFrames e use `left`.
- Cuidado com "explosão de join": se o lado direito tem múltiplas correspondências
  para a chave, as linhas se multiplicam. Confirme que a chave é única, a menos
  que a multiplicação seja esperada.
- Para colisão de nomes, **não** renomeie todas as colunas; apelide o DataFrame
  inteiro (`.alias(...)`) e selecione pelas colunas qualificadas. Resolva colunas
  ambíguas antes de gravar o dataset.
- **Não** use `.dropDuplicates()` nem `.distinct()` como muleta para duplicatas
  inesperadas — há quase sempre uma causa a investigar; mascará-la só adiciona
  custo.

## Funções de janela
- **Sempre especifique um frame explícito** (`rowsBetween`/`rangeBetween`). Sem
  frame, o Spark gera um implícito que muda conforme a janela é ordenada ou não —
  fonte de resultados difíceis de prever.

```python
w = W.partitionBy("key").orderBy("num").rowsBetween(W.unboundedPreceding, W.currentRow)
```

- Para funções analíticas (`F.first`, `F.last`, `F.lead`), lembre que nulls
  afetam o resultado: use `ignorenulls=True` quando fizer sentido e ordene nulls
  explicitamente (`F.asc_nulls_first` / `F.asc_nulls_last`).
- **Evite `W.partitionBy()` vazio** (frame global): força tudo em uma única
  partição. Prefira agregação:

```python
# ruim
df = df.select(F.sum("num").over(W.partitionBy()).alias("sum"))

# bom
df = df.agg(F.sum("num").alias("sum"))
```

## Encadeamento e legibilidade
- Não misture tipos diferentes de operação no mesmo encadeamento (ex.: criação
  de coluna + join + select + filter). Separe em blocos lógicos, cada um
  reatribuindo `df = df...`.
- Limite cada cadeia a no máximo **5** expressões. Cadeias com mais de 3
  operações já são fortes candidatas a virar uma função nomeada.
- Quebre expressões multi-linha envolvendo tudo em **um par de parênteses**;
  não use `\`.

```python
# ruim
df = df.filter(F.col("event") == "executing")\
    .filter(F.col("has_tests") == True)\
    .drop("has_tests")

# bom
df = (
    df
    .filter(F.col("event") == "executing")
    .filter(F.col("has_tests") == True)
    .drop("has_tests")
)
```

- Uma transformação lógica por função; nomes descritivos no imperativo que
  capturam o que a função **faz**, não os objetos que ela usa.

## Comentários
- O código deve ser legível por si. Se você precisa de comentários para explicar
  a lógica passo a passo, refatore.
- Comente o **porquê** (contexto, decisões, peculiaridades do dado), não o
  **o quê**. Em PySpark isso é especialmente valioso: quem lê entende o código,
  mas raramente conhece o dado que o alimenta.
- **Não** deixe código comentado no repositório — confie no git para histórico.

## Geoespacial (Sedona)
- Use a API Python do Sedona em vez de montar expressões SQL à mão, sempre que
  a operação existir na API Python (mesma lógica da regra de `F.expr`).
- Para distâncias em metros/km a partir de lat/long, use **distância geodésica**
  (`ST_DistanceSpheroid`, sobre o elipsoide WGS84), não distância euclidiana em graus.
- **Meça a distância sobre as coordenadas WGS84 ORIGINAIS, ANTES de projetar.** Não
  reprojete o ponto para uma camada métrica (ex.: EPSG:3857) e de volta só para medir:
  o round-trip `4326→3857→4326` perde precisão (sub-10 m nas latitudes do Brasil).
  Gravar a geometria em 3857 é convenção de **saída** e não muda o método de cálculo —
  meça primeiro sobre o ponto original, projete a geometria depois. Não use a distância
  planar da projeção (distorce os metros nas latitudes do Brasil).

## Saída
- Todo job termina **persistindo** o(s) DataFrame(s) resultante(s) no data
  lake, na camada correspondente à natureza da transformação.

## Outras recomendações
- Um arquivo não deve passar de ~250 linhas; uma função, de ~70 linhas.
- Ao integrar várias tabelas em uma transformação grande, quebre nos sub-passos
  naturais e extraia para funções — favorece legibilidade e reúso.
- Evite literais soltos (strings, inteiros) em filtros e novos valores de coluna;
  extraia-os para variáveis, constantes ou dicts que capturem o significado.
- Mantenha os aliases de import estabelecidos:
  `from pyspark.sql import types as T, functions as F`.
- Teste o código. Rodando os testes locais ou verificando manualmente que o dado
  saiu como esperado.
