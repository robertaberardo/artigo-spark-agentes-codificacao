from __future__ import annotations

from functools import partial, reduce

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, NullType, StructType
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf
from sedona.spark.sql.st_aggregates import ST_Union_Aggr
from sedona.spark.sql.st_predicates import ST_Intersects

from utils.column_transforms import (
    clear_string,
    replace_multiples,
    st_intersection_area_safe,
    surrogate_key,
)

# Limites aproximados do território brasileiro (validação de coordenadas).
BRAZIL_LAT_MIN, BRAZIL_LAT_MAX = -34.0, 6.0
BRAZIL_LON_MIN, BRAZIL_LON_MAX = -74.0, -33.0


def drop_high_null_columns(df: DataFrame, threshold: float = 0.9) -> DataFrame:
    """Remove colunas cuja proporção de nulos/vazios excede ``threshold``.

    Faz uma única passada para medir a taxa de nulos (contando também strings
    vazias) e descarta as colunas acima do limite. Preserva a ordem original das
    colunas mantidas.
    """
    total = df.count()
    if total == 0:
        return df
    null_rate = df.select(
        [
            (
                F.sum(
                    F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0)
                )
                / F.lit(total)
            ).alias(c)
            for c in df.columns
        ]
    ).collect()[0].asDict()
    keep = [c for c in df.columns if (null_rate[c] or 0.0) <= threshold]
    return df.select(*keep)


def deduplicate_by_key(df: DataFrame, keys: list[str]) -> DataFrame:
    """Remove linhas duplicadas considerando apenas as colunas-chave ``keys``.

    Mantém uma linha por combinação de chave. Use quando a duplicidade deve ser
    avaliada por um subconjunto de colunas, não pela linha inteira.
    """
    return df.dropDuplicates(keys)


def filter_valid_coordinates(
    df: DataFrame, lat_col: str = "lat", lon_col: str = "lon"
) -> DataFrame:
    """Mantém apenas linhas com coordenadas presentes e dentro do Brasil.

    Descarta nulos e pontos fora dos limites plausíveis do território, evitando
    que coordenadas sujas contaminem operações geoespaciais.
    """
    lat = F.col(lat_col)
    lon = F.col(lon_col)
    return df.filter(
        lat.isNotNull()
        & lon.isNotNull()
        & lat.between(BRAZIL_LAT_MIN, BRAZIL_LAT_MAX)
        & lon.between(BRAZIL_LON_MIN, BRAZIL_LON_MAX)
    )


def add_point_geometry(
    df: DataFrame,
    lat_col: str = "lat",
    lon_col: str = "lon",
    out_col: str = "geometry",
) -> DataFrame:
    """Adiciona uma coluna de geometria de ponto a partir de lat/long.

    Usa ``ST_Point`` (Sedona) com a ordem correta (x=longitude, y=latitude).
    Requer ``SedonaContext`` ativa.
    """
    return df.withColumn(out_col, stc.ST_Point(F.col(lon_col), F.col(lat_col)))


def add_distance_to_reference(
    df: DataFrame,
    ref_lat: float,
    ref_lon: float,
    geom_col: str = "geometry",
    out_col: str = "distance_m",
) -> DataFrame:
    """Adiciona a distância (em metros) de cada geometria a um ponto de referência.

    Usa ``ST_DistanceSpheroid`` (Sedona), que calcula a distância geodésica sobre
    o elipsoide WGS84 — adequado para lat/long em graus, ao contrário da
    distância euclidiana. Requer ``SedonaContext`` ativa.
    """
    reference = stc.ST_Point(F.lit(ref_lon), F.lit(ref_lat))
    return df.withColumn(out_col, stf.ST_DistanceSpheroid(F.col(geom_col), reference))


# --- Utilitárias de DataFrame (união / colunas / preenchimento) --------------

def union_dataframes(df_list: list[DataFrame], keep_all_columns: bool = True) -> DataFrame:
    """Une DataFrames do Spark com colunas/schemas variáveis.

    Com ``keep_all_columns=True`` mantém a união de todas as colunas (preenchendo
    ausentes com ``NULL``); com ``False`` mantém só as colunas comuns. Antes do
    ``unionByName`` valida, por posição, que os tipos não nulos coincidem e
    levanta ``TypeError`` listando todas as incompatibilidades encontradas.
    """
    if keep_all_columns:
        all_cols = list(set().union(*[df.columns for df in df_list]))
        aligned = []
        for df in df_list:
            for col in set(all_cols) - set(df.columns):
                df = df.withColumn(col, F.lit(None))
            aligned.append(df.selectExpr(*[f"`{c}`" for c in all_cols]))
        df_list = aligned
    else:
        common_cols = list(set.intersection(*map(set, [df.columns for df in df_list])))
        df_list = [df.selectExpr(*[f"`{c}`" for c in common_cols]) for df in df_list]

    # valida compatibilidade de tipos por posição, ignorando NullType
    errors = []
    for idx in range(len(df_list[0].columns)):
        col_name = df_list[0].columns[idx]
        non_null_types = [
            df.schema.fields[idx].dataType
            for df in df_list
            if not isinstance(df.schema.fields[idx].dataType, NullType)
        ]
        for dtype in non_null_types[1:]:
            if dtype != non_null_types[0]:
                errors.append(
                    f"Coluna '{col_name}' (posição {idx}): esperado "
                    f"{non_null_types[0]}, encontrado {dtype}"
                )
    if errors:
        raise TypeError("Incompatibilidades de tipo na união:\n" + "\n".join(errors))

    union_partial = partial(DataFrame.unionByName, allowMissingColumns=keep_all_columns)
    return reduce(union_partial, df_list)


def union_update_dataframes(
    df_a: DataFrame, df_b: DataFrame, ref_column: str | list[str]
) -> DataFrame:
    """Atualiza ``df_a`` com os dados de ``df_b`` casando por ``ref_column``.

    Mantém as linhas de ``df_a`` sem correspondência, sobrescreve as coincidentes
    com os valores de ``df_b`` e acrescenta as linhas novas de ``df_b`` (``outer
    join`` + ``coalesce``). Se ``df_b`` for vazio/``None``, devolve ``df_a``.
    """
    if not df_b:
        return df_a
    return df_a.alias("first").join(
        df_b.alias("other"), on=ref_column, how="outer"
    ).select(
        *[
            F.coalesce(F.col(f"`other`.`{c}`"), F.col(f"`first`.`{c}`")).alias(c)
            if c in df_b.columns
            else F.col(f"`first`.`{c}`")
            for c in df_a.columns
        ]
    )


def df_with_exploded_map_cols(
    df: DataFrame, map_cols: list[str], drop_original: bool = False
) -> DataFrame:
    """Transforma as chaves de colunas ``map`` em novas colunas do DataFrame.

    Coleta as chaves distintas de ``map_cols`` e cria uma coluna por chave
    (``coluna[chave]``). Com ``drop_original=True`` remove as colunas ``map`` de
    origem.
    """
    keys = (
        df.select(*[F.map_keys(c) for c in map_cols])
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    df = df.withColumns(
        {
            k.replace(".", ""): F.col(c).getItem(k)
            for i, c in enumerate(map_cols)
            for k in keys[i] or []
        }
    )
    return df.drop(*map_cols) if drop_original else df


def forward_fill_columns(
    df: DataFrame, col_names: list[str], order_by_col: str
) -> DataFrame:
    """Preenche nulos propagando o último valor não nulo (forward fill).

    Ordena por ``order_by_col`` e, para cada coluna em ``col_names``, repassa o
    último valor não nulo anterior. Corresponde ao ``ffill`` do pandas. (Versão
    nível-DataFrame; para uma expressão de coluna, ver ``column_transforms.
    forward_fill``.)
    """
    df = df.withColumn("_ffill_tmp", F.lit("tmp"))
    window = (
        Window.partitionBy("_ffill_tmp")
        .orderBy(order_by_col)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    filled = {c: F.last(F.col(c), ignorenulls=True).over(window) for c in col_names}
    return df.withColumns(filled).drop("_ffill_tmp")


# --- Geoespaciais (Sedona) ---------------------------------------------------

def df_divided_by_distance(
    df: DataFrame, max_length: float, geom_col: str = "geometry"
) -> DataFrame:
    """Divide cada LineString em segmentos que respeitam um comprimento máximo.

    A geometria de ``geom_col`` deve ser LineString; o limite depende do CRS do
    DataFrame. Acrescenta as colunas ``line_id`` (identifica a linha original),
    ``line_total_segments`` (em quantas partes foi dividida) e ``line_segment``
    (número do segmento). Requer ``SedonaContext`` ativa.
    """
    id_col, total_col, segment_col = "line_id", "line_total_segments", "line_segment"
    window = Window.partitionBy(id_col).orderBy(id_col)
    return (
        df.withColumns(
            {
                id_col: F.monotonically_increasing_id(),
                total_col: F.greatest(
                    F.floor(stf.ST_Length(geom_col) / F.lit(max_length)), F.lit(1)
                ).cast("int"),
            }
        )
        .withColumns(
            {
                "line_clones": F.explode(F.array_repeat(id_col, total_col)),
                segment_col: F.row_number().over(window),
            }
        )
        .withColumn(
            geom_col,
            stf.ST_LineSubstring(
                line_string=geom_col,
                start_fraction=(F.col(segment_col) - 1) / F.col(total_col),
                end_fraction=F.col(segment_col) / F.col(total_col),
            ),
        )
        .drop("line_clones")
    )


def calculate_intersection_ratio(
    df: DataFrame,
    geom_a: Column,
    geom_b: Column,
    unique_col: Column,
    new_col_prefix: str,
    condition: Column | None = None,
    default_value: Column | None = None,
) -> DataFrame:
    """Razão de interseção entre ``geom_a`` e ``geom_b``, com redistribuição.

    A área fora da interseção é redistribuída proporcionalmente às partes que se
    interseccionam, por grupo de ``unique_col``. Cria a coluna
    ``{new_col_prefix}_intersection_ratio``. Só as linhas que satisfazem
    ``condition`` entram no cálculo; as demais recebem ``default_value`` (default
    ``1.0``). Reusa :func:`utils.column_transforms.st_intersection_area_safe`.
    Requer ``SedonaContext`` ativa.
    """
    condition = condition if condition is not None else F.lit(True)
    default_value = default_value if default_value is not None else F.lit(1.0)
    ratio_col = f"{new_col_prefix}_intersection_ratio"

    kept = df.filter(condition)
    skipped = df.filter(~condition).withColumn(ratio_col, default_value)

    window = Window.partitionBy(unique_col)
    intersect_ratio = st_intersection_area_safe(geom_a, geom_b) / stf.ST_Area(geom_a)
    intersect_sum = F.sum(intersect_ratio).over(window)
    proportion = intersect_ratio / intersect_sum
    remaining = F.lit(1) - intersect_sum
    redistributed = (intersect_ratio + proportion * remaining).cast("decimal(3,2)")

    return kept.withColumn(ratio_col, redistributed).union(skipped)


def translate_sdf_geometry(
    df: DataFrame,
    geometry_col: str,
    angle: float = 0.0,
    dx: float = 0.0,
    dy: float = 0.0,
) -> DataFrame:
    """Aplica rotação e/ou translação às geometrias, em torno do centróide global.

    Só aplica as transformações cujos parâmetros diferem do default. O resultado
    vai para a coluna ``geometry_transformed``. Requer ``SedonaContext`` ativa.
    """
    centroid = df.selectExpr(
        f"ST_Centroid(ST_Union_Aggr({geometry_col})) as centroid"
    ).collect()[0][0]
    cx, cy = centroid.x, centroid.y

    geom_expr = geometry_col
    if angle != 0:
        geom_expr = f"ST_Rotate({geom_expr}, {angle}, {cx}, {cy})"
    if dx != 0 or dy != 0:
        geom_expr = f"ST_Translate({geom_expr}, {dx}, {dy})"

    return df.withColumn("geometry_transformed", F.expr(geom_expr))


# --- Flatten de estruturas aninhadas / JSON ----------------------------------

def flatten_nested_columns(df: DataFrame) -> DataFrame:
    """Achata recursivamente todas as colunas ``StructType``/``ArrayType``.

    Structs viram colunas ``pai>filho`` (caminho completo) e arrays são expandidos
    com ``explode_outer``, repetindo até não restar coluna aninhada.
    """
    def nested(d: DataFrame) -> dict:
        return {
            field.name: field.dataType
            for field in d.schema.fields
            if isinstance(field.dataType, (ArrayType, StructType))
        }

    fields = nested(df)
    while fields:
        name, dtype = next(iter(fields.items()))
        if isinstance(dtype, StructType):
            children = [
                F.col(f"{name}.{child.name}").alias(f"{name}>{child.name}")
                for child in dtype
            ]
            df = df.select("*", *children).drop(name)
        else:  # ArrayType
            df = df.withColumn(name, F.explode_outer(name))
        fields = nested(df)
    return df


def flatten_json_column(
    df: DataFrame,
    json_column_name: str,
    spark: SparkSession,
    primary_partition_column: str | None = None,
) -> DataFrame:
    """Faz o parse de uma coluna string JSON e achata o resultado em colunas.

    Infere o schema do JSON contido em ``json_column_name`` (lendo-o com a
    ``SparkSession``), converte em struct e aplica :func:`flatten_nested_columns`.
    ``primary_partition_column``, se informada, é descartada antes do parse.
    """
    sentinel = "transformedJSON"
    base = df if primary_partition_column is None else df.drop(primary_partition_column)
    wrapped = base.withColumn(
        "_wrapped_json",
        F.concat(F.lit('{"' + sentinel + '":'), base[json_column_name], F.lit("}")),
    )
    json_schema = spark.read.json(wrapped.rdd.map(lambda row: row._wrapped_json)).schema
    parsed = (
        wrapped.drop(json_column_name)
        .withColumn(json_column_name, F.from_json(F.col("_wrapped_json"), json_schema))
        .drop("_wrapped_json")
        .select(f"{json_column_name}.*", "*")
        .drop(json_column_name)
    )
    flattened = flatten_nested_columns(parsed)
    return flattened.toDF(
        *[c.replace(sentinel, json_column_name) for c in flattened.columns]
    )


# --- Adaptadas do toolbox externo --------------------------------------------

def self_split_on_intersections(
    df: DataFrame, geometry_col: str, id_col: str = "ref_id"
) -> DataFrame:
    """Divide geometrias que se intersectam entre si dentro do mesmo DataFrame.

    Faz um self-join espacial por ``ST_Intersects`` (excluindo a própria linha via
    ``id_col``), agrega as geometrias que cruzam cada linha e usa ``ST_Split`` para
    recortá-la. Requer ``SedonaContext`` ativa e uma coluna identificadora
    (``id_col``).
    """
    intersection_points = (
        df.alias("l")
        .join(
            df.alias("r"),
            ST_Intersects(F.col(f"l.{geometry_col}"), F.col(f"r.{geometry_col}"))
            & (F.col(f"l.{id_col}") != F.col(f"r.{id_col}")),
        )
        .groupBy(f"l.{id_col}")
        .agg(ST_Union_Aggr(F.col(f"r.{geometry_col}")).alias(geometry_col))
    )
    return (
        df.alias("l")
        .join(
            intersection_points.alias("r"),
            F.col(f"l.{id_col}") == F.col(f"r.{id_col}"),
            "left",
        )
        .select(
            *[f"l.{c}" for c in df.columns if c != geometry_col],
            F.explode_outer(
                stf.ST_Dump(
                    stf.ST_Split(F.col(f"l.{geometry_col}"), F.col(f"r.{geometry_col}"))
                )
            ).alias(geometry_col),
        )
    )


def create_h3_grid_from_geom(
    df: DataFrame,
    geom_col: str,
    level: int,
    full_cover: bool = False,
    explode_df: bool = True,
    crs: str = "EPSG:3857",
) -> DataFrame:
    """Gera uma grade de hexágonos H3 cobrindo a geometria de ``geom_col``.

    ``level`` (1–15) controla o tamanho do hexágono (maior = menor/mais preciso,
    cresce exponencialmente). ``full_cover=True`` garante cobertura total (pode
    gerar redundância). Com ``explode_df=True`` retorna uma linha por hexágono
    (colunas ``hex_cell_id``/``hex_geom``); com ``False`` agrega-os em ``hex_data``
    (array de structs). ``crs`` é o CRS da geometria de entrada — a grade é
    calculada em EPSG:4326 e reprojetada de volta. Requer ``SedonaContext`` ativa.
    """
    hex_df = (
        df.withColumns(
            {
                geom_col: stf.ST_Transform(F.col(geom_col), F.lit(crs), F.lit("EPSG:4326")),
                "row_id": F.monotonically_increasing_id(),
            }
        )
        .withColumn(
            "hex_cell_ids",
            stf.ST_H3CellIDs(geometry=geom_col, level=level, full_cover=full_cover),
        )
        .withColumn("hex_geoms", stf.ST_H3ToGeom(cells=F.col("hex_cell_ids")))
    )

    hex_df = (
        hex_df.select(
            "*",
            F.posexplode(F.col("hex_cell_ids")).alias("pos_hex_cell_id", "hex_cell_id"),
        )
        .withColumn("hex_geom", F.col("hex_geoms")[F.col("pos_hex_cell_id")])
        .withColumns(
            {
                geom_col: stf.ST_Transform(F.col(geom_col), F.lit("EPSG:4326"), F.lit(crs)),
                "hex_geom": stf.ST_Transform(F.col("hex_geom"), F.lit("EPSG:4326"), F.lit(crs)),
            }
        )
    )

    helper_cols = {
        "hex_cell_ids", "hex_cell_id", "hex_geoms", "hex_geom", "pos_hex_cell_id", "row_id"
    }
    if not explode_df:
        return hex_df.groupBy("row_id").agg(
            F.collect_list(
                F.struct(F.col("hex_cell_id"), F.col("hex_geom"))
            ).alias("hex_data"),
            *[F.first(F.col(c)).alias(c) for c in hex_df.columns if c not in helper_cols],
        ).drop("row_id")
    return hex_df.drop("hex_cell_ids", "hex_geoms", "pos_hex_cell_id", "row_id")


def cluster_by_soundex(
    df: DataFrame, column: Column, out_column: str = "soundex_cluster_id"
) -> DataFrame:
    """Agrupa strings semelhantes via Soundex, ignorando as palavras mais comuns.

    Normaliza o texto (:func:`utils.column_transforms.clear_string` + minúsculas),
    remove as 10 palavras mais frequentes do conjunto (via
    :func:`utils.column_transforms.replace_multiples`) e aplica ``F.soundex`` para
    gerar o id do cluster em ``out_column``.
    """
    normalized_col = "_soundex_normalized"
    df = df.withColumn(normalized_col, F.lower(clear_string(column)))

    common_words = (
        df.select(F.explode(F.split(F.col(normalized_col), " ")).alias("word"))
        .groupBy("word")
        .count()
        .sort("count", ascending=False)
        .limit(10)
    )
    to_exclude = [row.word for row in common_words.collect()]

    return df.withColumn(
        out_column,
        F.soundex(replace_multiples(to_replace=to_exclude, new_val="")(normalized_col)),
    ).drop(normalized_col)


# --- Reprojeção e CRS (Sedona) -----------------------------------------------

def reproject_geometry(
    df: DataFrame,
    geom_col: str = "geometry",
    from_crs: str = "EPSG:4326",
    to_crs: str = "EPSG:3857",
) -> DataFrame:
    """Reprojeta a geometria de ``geom_col`` de um CRS de origem para outro.

    Usa ``ST_Transform`` (Sedona). O default vai de WGS84 (graus) para Web
    Mercator (metros), a projeção usada para medidas planares e para as grades
    H3 do lake. Requer ``SedonaContext`` ativa.
    """
    return df.withColumn(
        geom_col, stf.ST_Transform(F.col(geom_col), F.lit(from_crs), F.lit(to_crs))
    )


def to_web_mercator(df: DataFrame, geom_col: str = "geometry") -> DataFrame:
    """Leva a geometria de WGS84 para EPSG:3857.

    Atalho de :func:`reproject_geometry` para o caso mais comum do silver
    geoespacial. Requer ``SedonaContext`` ativa.
    """
    return reproject_geometry(df, geom_col, "EPSG:4326", "EPSG:3857")


def add_line_geometry(
    df: DataFrame, wkt_col: str, out_col: str = "geometry"
) -> DataFrame:
    """Constrói uma geometria (LineString/qualquer) a partir de uma coluna WKT.

    Usa ``ST_GeomFromWKT`` (Sedona). Requer ``SedonaContext`` ativa.
    """
    return df.withColumn(out_col, stc.ST_GeomFromWKT(F.col(wkt_col)))


def add_area_km2(
    df: DataFrame, geom_col: str = "geometry", out_col: str = "area_km2"
) -> DataFrame:
    """Adiciona a área da geometria em km².

    ``ST_Area`` devolve a área na unidade do CRS; para km² a geometria deve estar
    num CRS métrico (ex.: EPSG:3857), e o resultado é dividido por 1e6. Requer
    ``SedonaContext`` ativa.
    """
    return df.withColumn(out_col, stf.ST_Area(F.col(geom_col)) / F.lit(1_000_000.0))


def bbox_filter(
    df: DataFrame,
    geom_col: str,
    min_x: float,
    min_y: float,
    max_x: float,
    max_y: float,
) -> DataFrame:
    """Mantém só as geometrias que caem dentro de um retângulo (bounding box).

    O envelope é montado como polígono WKT e o filtro usa ``ST_Intersects``. As
    coordenadas do box devem estar no mesmo CRS da geometria. Requer
    ``SedonaContext`` ativa.
    """
    envelope = stc.ST_PolygonFromEnvelope(
        F.lit(min_x), F.lit(min_y), F.lit(max_x), F.lit(max_y)
    )
    return df.filter(ST_Intersects(F.col(geom_col), envelope))


# --- H3 e agregação espacial -------------------------------------------------

def attach_h3_index(
    df: DataFrame, geom_col: str = "geometry", level: int = 7, out_col: str = "h3_cell"
) -> DataFrame:
    """Anexa o id da célula H3 que contém a geometria, no nível pedido.

    Usa ``ST_H3CellIDs`` (Sedona) e toma a primeira célula. Requer
    ``SedonaContext`` ativa.
    """
    return df.withColumn(
        out_col,
        F.element_at(stf.ST_H3CellIDs(F.col(geom_col), level, False), 1),
    )


def count_h3_kring_members(
    df: DataFrame,
    cell_col: str,
    k: int = 1,
    out_col: str = "n_kring_members",
    exact_ring: bool = False,
) -> DataFrame:
    """Conta, por célula H3, quantas linhas caem na sua vizinhança de raio ``k``.

    Para cada célula distinta de ``cell_col``, soma os membros das células no
    k-ring H3 (com ``exact_ring=False`` inclui a própria célula e todas as
    adjacentes até ``k``). Devolve ``cell_col`` e ``out_col`` — o tamanho da
    vizinhança, idêntico para todas as linhas de uma mesma célula. Requer
    ``SedonaContext`` ativa.
    """
    members = df.groupBy(cell_col).agg(F.count(F.lit(1)).alias("_kring_members"))
    ring = members.select(
        F.col(cell_col).alias("_kring_home"),
        F.explode(
            stf.ST_H3KRing(F.col(cell_col), F.lit(k), F.lit(exact_ring))
        ).alias("_kring_cell"),
    )
    return (
        ring.join(members, ring["_kring_cell"] == members[cell_col], "left")
        .groupBy("_kring_home")
        .agg(F.sum("_kring_members").alias(out_col))
        .select(F.col("_kring_home").alias(cell_col), F.col(out_col))
    )


def h3_kring_neighbor_distances(
    df: DataFrame,
    id_col: str,
    cell_col: str,
    geom_col: str = "geometry",
    k: int = 1,
    out_col: str = "dist_neighbors_m",
) -> DataFrame:
    """Distância geodésica média de cada linha às suas vizinhas no k-ring H3.

    São vizinhas as demais linhas cuja célula (``cell_col``) está no k-ring da
    célula da linha (própria célula + adjacentes, com ``k=1``). A média usa
    ``ST_DistanceSpheroid`` (geodésica, em metros), pelo que ``geom_col`` deve
    estar em WGS84. Devolve ``id_col``, ``cell_col`` e ``out_col``; linhas sem
    vizinha não aparecem. Requer ``SedonaContext`` ativa.
    """
    left = df.select(
        F.col(id_col).alias("_nbr_a_id"),
        F.col(cell_col).alias("_nbr_home"),
        F.col(geom_col).alias("_nbr_a_geom"),
    ).withColumn(
        "_nbr_ring",
        F.explode(stf.ST_H3KRing(F.col("_nbr_home"), F.lit(k), F.lit(False))),
    )
    right = df.select(
        F.col(id_col).alias("_nbr_b_id"),
        F.col(cell_col).alias("_nbr_b_cell"),
        F.col(geom_col).alias("_nbr_b_geom"),
    )
    other = F.col("_nbr_a_id") != F.col("_nbr_b_id")
    pairs = left.join(
        right, F.col("_nbr_ring") == F.col("_nbr_b_cell"), "inner"
    ).filter(other)
    distance = stf.ST_DistanceSpheroid(F.col("_nbr_a_geom"), F.col("_nbr_b_geom"))
    return (
        pairs.withColumn("_nbr_dist", distance)
        .groupBy("_nbr_a_id", "_nbr_home")
        .agg(F.avg("_nbr_dist").alias(out_col))
        .select(
            F.col("_nbr_a_id").alias(id_col),
            F.col("_nbr_home").alias(cell_col),
            F.col(out_col),
        )
    )


def nearest_neighbor_distance(
    df: DataFrame,
    id_col: str,
    geom_col: str = "geometry",
    out_col: str = "dist_nearest_m",
) -> DataFrame:
    """Distância geodésica de cada linha à linha mais próxima do conjunto inteiro.

    Faz um cross-join do conjunto consigo mesmo (adequado a conjuntos pequenos),
    mede com ``ST_DistanceSpheroid`` (em metros, sobre WGS84) e fica com a menor
    por linha. Devolve ``id_col`` e ``out_col``. Requer ``SedonaContext`` ativa.
    """
    left = df.select(
        F.col(id_col).alias("_nn_a_id"), F.col(geom_col).alias("_nn_a_geom")
    )
    right = df.select(
        F.col(id_col).alias("_nn_b_id"), F.col(geom_col).alias("_nn_b_geom")
    )
    pairs = left.crossJoin(right).filter(F.col("_nn_a_id") != F.col("_nn_b_id"))
    distance = stf.ST_DistanceSpheroid(F.col("_nn_a_geom"), F.col("_nn_b_geom"))
    return (
        pairs.withColumn("_nn_dist", distance)
        .groupBy("_nn_a_id")
        .agg(F.min("_nn_dist").alias(out_col))
        .select(F.col("_nn_a_id").alias(id_col), F.col(out_col))
    )


def aggregate_by_h3(
    df: DataFrame, h3_col: str, agg_exprs: list[Column]
) -> DataFrame:
    """Agrega por célula H3 e reconstrói a geometria da célula.

    Agrupa por ``h3_col``, aplica ``agg_exprs`` e devolve a coluna ``geometry``
    da célula via ``ST_H3ToGeom``. Requer ``SedonaContext`` ativa.
    """
    return (
        df.groupBy(h3_col)
        .agg(*agg_exprs)
        .withColumn(
            "geometry", F.element_at(stf.ST_H3ToGeom(F.array(F.col(h3_col))), 1)
        )
    )


def h3_join(
    df: DataFrame,
    level: int,
    geom_col: str = "geometry",
    crs: str = "EPSG:4326",
    cell_col: str = "h3_cell",
    cell_geom_col: str = "h3_geom",
) -> DataFrame:
    """Associa cada geometria à célula H3 do nível pedido e anexa a célula.

    Faz o "join" dos pontos à grade H3: calcula a célula que contém a geometria
    (``ST_H3CellIDs``, nível 0–15) e reconstrói o polígono da célula
    (``ST_H3ToGeom``). Acrescenta ``cell_col`` (id da célula) e ``cell_geom_col``
    (polígono da célula, no mesmo CRS de ``crs``). O H3 opera em coordenadas
    geográficas; quando ``crs`` não é ``EPSG:4326`` a geometria é reprojetada
    apenas para o cálculo, sem alterar ``geom_col``. Requer ``SedonaContext`` ativa.
    """
    h3_input = F.col(geom_col)
    if crs != "EPSG:4326":
        h3_input = stf.ST_Transform(F.col(geom_col), F.lit(crs), F.lit("EPSG:4326"))

    with_cell = df.withColumn(
        cell_col, F.element_at(stf.ST_H3CellIDs(h3_input, level, False), 1)
    )
    cell_geom = F.element_at(stf.ST_H3ToGeom(F.array(F.col(cell_col))), 1)
    if crs != "EPSG:4326":
        cell_geom = stf.ST_Transform(cell_geom, F.lit("EPSG:4326"), F.lit(crs))
    return with_cell.withColumn(cell_geom_col, cell_geom)


# --- Janela: ranking, participação, médias móveis ----------------------------

def rank_within_group(
    df: DataFrame,
    partition_cols: list[str],
    order_col: str,
    out_col: str = "rank",
    ascending: bool = False,
) -> DataFrame:
    """Adiciona o ranking de ``order_col`` dentro de cada grupo de partição.

    ``ascending=False`` coloca o maior valor em 1º. Empates recebem posições
    consecutivas (``rank`` do Spark, com saltos).
    """
    order = F.col(order_col).asc() if ascending else F.col(order_col).desc()
    window = Window.partitionBy(*partition_cols).orderBy(order)
    return df.withColumn(out_col, F.rank().over(window))


def add_group_share(
    df: DataFrame,
    value_col: str,
    partition_cols: list[str],
    out_col: str = "share",
) -> DataFrame:
    """Fração de ``value_col`` sobre o total do grupo (particionado)."""
    window = Window.partitionBy(*partition_cols)
    return df.withColumn(
        out_col, F.col(value_col) / F.sum(value_col).over(window)
    )


def add_percentile_rank(
    df: DataFrame,
    value_col: str,
    partition_cols: list[str],
    out_col: str = "pct_rank",
) -> DataFrame:
    """Percentil (0..1) de ``value_col`` dentro de cada grupo (``percent_rank``)."""
    window = Window.partitionBy(*partition_cols).orderBy(F.col(value_col).asc())
    return df.withColumn(out_col, F.percent_rank().over(window))


def add_moving_average(
    df: DataFrame,
    value_col: str,
    order_col: str,
    window_size: int,
    out_col: str = "moving_avg",
    partition_cols: list[str] | None = None,
) -> DataFrame:
    """Média móvel de ``value_col`` sobre as últimas ``window_size`` linhas.

    Ordena por ``order_col`` (dentro de ``partition_cols``, se informado) e faz a
    média da janela deslizante que termina na linha atual.
    """
    base = Window.orderBy(order_col)
    if partition_cols:
        base = Window.partitionBy(*partition_cols).orderBy(order_col)
    window = base.rowsBetween(-(window_size - 1), Window.currentRow)
    return df.withColumn(out_col, F.avg(value_col).over(window))


def add_running_total(
    df: DataFrame,
    value_col: str,
    order_col: str,
    out_col: str = "running_total",
    partition_cols: list[str] | None = None,
) -> DataFrame:
    """Soma acumulada de ``value_col`` ao longo de ``order_col``."""
    base = Window.orderBy(order_col)
    if partition_cols:
        base = Window.partitionBy(*partition_cols).orderBy(order_col)
    window = base.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return df.withColumn(out_col, F.sum(value_col).over(window))


def dedupe_keep_latest(
    df: DataFrame, keys: list[str], order_col: str
) -> DataFrame:
    """Mantém, por combinação de ``keys``, apenas a linha mais recente.

    Usa ``row_number`` ordenando ``order_col`` de forma decrescente e fica com a
    primeira linha de cada chave.
    """
    window = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# --- Utilitárias de DataFrame (chaves, casts, lookup, datas) ------------------

def add_surrogate_key(
    df: DataFrame, columns: list[str], out_col: str = "sk"
) -> DataFrame:
    """Adiciona uma chave substituta (:func:`utils.column_transforms.surrogate_key`)."""
    return df.withColumn(out_col, surrogate_key(columns))


def cast_columns(df: DataFrame, type_map: dict) -> DataFrame:
    """Faz cast em lote das colunas de ``type_map`` (``{coluna: tipo}``)."""
    return df.withColumns({c: F.col(c).cast(t) for c, t in type_map.items()})


def filter_by_date_range(
    df: DataFrame, date_col: str, start: str, end: str
) -> DataFrame:
    """Mantém só as linhas com ``date_col`` no intervalo ``[start, end]`` (inclusive)."""
    return df.filter(F.col(date_col).between(F.lit(start), F.lit(end)))


def join_lookup(
    df: DataFrame,
    lookup_df: DataFrame,
    on: str | list[str],
    columns: list[str] | None = None,
    how: str = "left",
    broadcast: bool = True,
) -> DataFrame:
    """Enriquece ``df`` com colunas de ``lookup_df`` casando por ``on``.

    Seleciona só ``columns`` (mais a chave) do lado de lookup e faz o join, com
    ``broadcast`` quando o lado de lookup é pequeno.
    """
    keys = [on] if isinstance(on, str) else on
    right = lookup_df
    if columns is not None:
        right = lookup_df.select(*keys, *columns)
    if broadcast:
        right = F.broadcast(right)
    return df.join(right, on=on, how=how)


def join_no_fanout(
    left: DataFrame,
    right: DataFrame,
    on: str | list[str],
    how: str,
    forbid_fanout: bool = True,
) -> DataFrame:
    """Junta dois DataFrames com ``how`` explícito e sem fan-out silencioso.

    Ao contrário de ``DataFrame.join``, exige que o chamador declare ``how`` (o
    ``inner`` implícito costuma esconder a intenção) e, com ``forbid_fanout=True``,
    valida que o lado direito é único na chave ``on`` — levanta ``ValueError`` em
    vez de deixar o join multiplicar linhas. Assim a duplicidade vira erro
    investigável, não uma muleta de ``dropDuplicates``/``distinct``.
    """
    keys = [on] if isinstance(on, str) else on
    if forbid_fanout:
        has_duplicate = (
            right.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count()
        )
        if has_duplicate:
            raise ValueError(
                f"lado direito duplicado na chave {keys} — join geraria fan-out"
            )
    return left.join(right, on=on, how=how)


def explode_delimited_column(
    df: DataFrame, col: str, delimiter: str = ";", out_col: str | None = None
) -> DataFrame:
    """Quebra uma coluna de texto por ``delimiter`` e explode em várias linhas."""
    target = out_col or col
    return df.withColumn(target, F.explode(F.split(F.col(col), delimiter)))


def pivot_key_value(
    df: DataFrame,
    group_cols: list[str],
    key_col: str,
    value_col: str,
) -> DataFrame:
    """Pivota pares chave/valor em colunas, somando os valores por grupo."""
    return df.groupBy(*group_cols).pivot(key_col).agg(F.sum(value_col))
