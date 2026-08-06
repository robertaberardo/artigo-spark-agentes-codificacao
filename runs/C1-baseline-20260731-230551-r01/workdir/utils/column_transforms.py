from __future__ import annotations

from typing import Any, Callable, List, Optional, Tuple

from pyspark.sql import Column, Window
from pyspark.sql import functions as F
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf


def blank_to_null(col: Column) -> Column:
    """Converte strings vazias ou só com espaços em ``NULL``.

    Útil porque os CSVs do Censo representam ausência ora como vazio, ora como
    espaços, e o tratamento de nulos precisa ser uniforme.
    """
    trimmed = F.trim(col)
    return F.when(trimmed == "", None).otherwise(trimmed)


def normalize_text(col: Column) -> Column:
    """Normaliza texto: remove espaços das pontas e colapsa espaços internos.

    Não altera acentuação nem caixa — apenas padroniza o espaçamento.
    """
    return F.trim(F.regexp_replace(col, r"\s+", " "))


def br_decimal_to_double(col: Column) -> Column:
    """Interpreta número no formato brasileiro (vírgula decimal) como ``double``.

    Remove separadores de milhar (``.``) e troca a vírgula decimal por ponto
    antes do cast. Retorna ``NULL`` quando o valor não é numérico.
    """
    cleaned = F.regexp_replace(F.regexp_replace(F.trim(col), r"\.", ""), ",", ".")
    return cleaned.cast("double")


def to_coordinate(col: Column) -> Column:
    """Converte uma coluna de coordenada (texto) em ``double``.

    Aceita ponto ou vírgula como separador decimal. A validação de limites
    (dentro do Brasil) é responsabilidade de ``filter_valid_coordinates`` no
    módulo de DataFrame, pois depende do par lat/long em conjunto.
    """
    return F.regexp_replace(F.trim(col), ",", ".").cast("double")


def make_point(lon_col: Column, lat_col: Column) -> Column:
    """Cria uma geometria de ponto (Sedona ``ST_Point``) a partir de lon/lat.

    Atenção à ordem: ``ST_Point(x, y)`` espera x=longitude e y=latitude. Requer
    uma ``SedonaContext`` ativa para que a função espacial esteja registrada.
    """
    return stc.ST_Point(lon_col, lat_col)


# --- Saneamento (nulos / valores ausentes) -----------------------------------

def nan_as_null(col: Column) -> Column:
    """Converte valores ``NaN`` em ``NULL``, preservando os demais."""
    return F.when(F.isnan(col), F.lit(None)).otherwise(col)


def has_value(col: Column) -> Column:
    """Indica (``True``/``False``) se a coluna tem valor útil.

    Trata como ausente: ``NULL``, ``NaN``, string vazia/só espaços e os literais
    de texto ``"null"``/``"nan"``. Assume valores comparáveis como texto.
    """
    missing = (
        (col == "null")
        | (col == "nan")
        | (F.trim(col) == "")
        | F.isnan(col)
        | col.isNull()
    )
    return F.when(missing, F.lit(False)).otherwise(F.lit(True))


def safely_convert_int(col: Column) -> Column:
    """Converte para inteiro só quando não há perda de informação.

    Mantém o valor original se o cast ida-e-volta mudar o dado (ex.: ``"01"``
    viraria ``"1"``). Nulos e ``NaN`` passam adiante inalterados.
    """
    lossless = col.cast("int").cast("string") == col
    return F.when(
        col.isNull() | F.isnan(col) | lossless, col.cast("int")
    ).otherwise(col)


def contains_null_safe(col: Column, to_find: str) -> Column:
    """Verifica se ``to_find`` ocorre na coluna, tratando ``NULL`` como ``False``.

    Diferente de ``Column.like``, não propaga ``NULL`` (usa ``coalesce`` para "").
    """
    return F.coalesce(col, F.lit("")).like(f"%{to_find}%")


# --- Texto -------------------------------------------------------------------

def clear_string(col: Column) -> Column:
    """Remove acentos e pontuação de um texto, colapsando espaços.

    Troca hífens, sublinhados, pontuação e apóstrofos por espaço, colapsa espaços
    repetidos, remove as bordas e substitui caracteres acentuados pelos
    equivalentes sem acento.
    """
    accented = "ãäâàöüẞáäčçďéěêíĺľňóôõŕšťúůýžÄÖÜẞÃÁÄÂČÇĎÉĚÊÍĹĽŇÓÔÕŔŠŤÚŮÝŽ"
    plain = "aaaaousaaccdeeeillnooorstuuyzAOUSAAAACCDEEEILLNOOORSTUUYZ"
    return F.translate(
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(col, r"[\-\_\>\<\.\,\&'´`’‘]", " "),
                r"\s\s+", " ",
            ),
            r"^\s+|\s+$", "",
        ),
        accented, plain,
    )


def replace_multiples(to_replace: list | dict, new_val: str | None = None):
    """Fábrica que substitui vários termos numa coluna via ``regexp_replace``.

    ``to_replace`` é uma lista (todos trocados por ``new_val``) ou um dicionário
    ``{antigo: novo}``. É 100% nativa (sem UDF), útil inclusive dentro de funções
    que não aceitam UDFs (ex.: ``F.transform``).
    """
    def inner(col: Column) -> Column:
        mapping = (
            to_replace if isinstance(to_replace, dict)
            else {term: new_val for term in to_replace}
        )
        for old, new in mapping.items():
            col = F.regexp_replace(col, old, new)
        return col

    return inner


# --- Arrays ------------------------------------------------------------------

def array_sum(col: Column) -> Column:
    """Soma os elementos de um array numérico."""
    return F.aggregate(col, F.lit(0.0), lambda acc, x: acc + x)


def array_sum_nan_safe(col: Column) -> Column:
    """Soma um array numérico tratando ``NULL``/``NaN`` como zero.

    Usa :func:`has_value` para ignorar ausências; se todos os elementos forem
    ausentes, o resultado é ``0.0``.
    """
    def null_as_zero(c: Column) -> Column:
        return F.when(has_value(c), c).otherwise(F.lit(0.0))

    return F.aggregate(
        col, F.lit(0.0), lambda acc, x: null_as_zero(acc) + null_as_zero(x)
    )


def array_mean(col: Column) -> Column:
    """Média de um array numérico, ignorando ``NULL``/``NaN`` na soma.

    O divisor é a quantidade de elementos não nulos (``array_compact``).
    """
    return array_sum_nan_safe(col) / F.size(F.array_compact(col))


def array_when_not_null(col: Column) -> Column:
    """Envolve o valor num array de um item, ou ``NULL`` se o valor for nulo."""
    return F.when(col.isNotNull(), F.array(col)).otherwise(F.lit(None))


def array_count_if(col: Column, condition: Callable[[Column], Column]) -> Column:
    """Conta os itens do array que satisfazem ``condition``.

    ``condition`` recebe o elemento (``Column``) e devolve uma expressão booleana.
    """
    flags = F.transform(
        col, lambda c: F.when(condition(c), F.lit(1)).otherwise(F.lit(0))
    )
    return array_sum(flags)


def array_flatten_null_safe(col: Column) -> Column:
    """Achata um array de arrays ignorando os sub-arrays ``NULL``.

    Equivale a ``F.flatten``, mas não retorna ``NULL`` quando há itens nulos.
    """
    return F.flatten(F.filter(col, lambda c: c.isNotNull()))


def array_indexes(col: Column) -> Column:
    """Cria um array com os índices dos itens (``[a, b, c] -> [0, 1, 2]``).

    Retorna ``NULL`` para arrays vazios.
    """
    return F.when(
        F.size(col) >= 1, F.sequence(F.lit(0), F.size(col) - 1)
    ).otherwise(F.lit(None))


def sum_columns(columns: list[Column]) -> Column:
    """Soma horizontalmente (por linha) uma lista de colunas."""
    total = columns[0]
    for col in columns[1:]:
        total = total + col
    return total


# --- Maps e structs ----------------------------------------------------------

def map_from_columns(columns: list[str]) -> Column:
    """Monta um ``map`` a partir dos nomes de colunas (``{nome: valor}``)."""
    entries: list[Column] = []
    for name in columns:
        entries.extend([F.lit(name), F.col(name)])
    return F.create_map(*entries)


def map_concat_null_safe(columns: list[Column]) -> Column:
    """Concatena colunas de ``map`` ignorando as nulas.

    Diferente de ``F.map_concat``, um ``map`` nulo não anula o resultado (é
    tratado como mapa vazio).
    """
    empty = F.create_map()
    return F.map_concat(*[F.coalesce(col, empty) for col in columns])


def struct_check_any_non_null(col: Column, fields: list[str]) -> Column:
    """Indica se algum dos ``fields`` da struct não é nulo."""
    condition = col[fields[0]].isNotNull()
    for field in fields[1:]:
        condition = condition | col[field].isNotNull()
    return condition


def array_filter_non_null_structs(col: Column, fields: list[str]) -> Column:
    """Mantém no array apenas as structs com ao menos um ``field`` não nulo."""
    return F.filter(col, lambda x: struct_check_any_non_null(x, fields))


def group_array_concat(col: Column) -> Column:
    """Agrega (em ``group by``) os arrays de várias linhas num único array.

    Junta os arrays coletados com :func:`array_flatten_null_safe`. Use dentro de
    ``agg``.
    """
    return array_flatten_null_safe(F.collect_list(col))


# --- Geometria (Sedona) ------------------------------------------------------

def validate_wkt_syntax(col: Column) -> Column:
    """Checa se a string segue a sintaxe de uma geometria WKT (via regex).

    Não garante que a geometria seja válida — apenas que a escrita é bem formada.
    """
    pattern = r"^[A-Z]+\s+(Z+\s)?(\(.*\)|EMPTY)$"
    return col.rlike(pattern)


def is_valid_wgs84_coordinate(col: Column, coordinate: str) -> Column:
    """Indica se a coluna tem coordenada WGS84 válida (``True``/``False``).

    ``coordinate`` deve ser ``"latitude"`` (-90..90) ou ``"longitude"``
    (-180..180). Nulos/valores fora do intervalo resultam em ``False``.
    """
    if coordinate == "latitude":
        limits = col.cast("double").between(-90, 90)
    elif coordinate == "longitude":
        limits = col.cast("double").between(-180, 180)
    else:
        raise ValueError("coordinate inválido: use 'latitude' ou 'longitude'.")
    return F.when(limits, F.lit(True)).otherwise(F.lit(False))


def fix_geometry(col: Column, return_geometry: bool = False) -> Column:
    """Tenta corrigir a geometria; se não der, retorna ``NULL`` ou geometria vazia.

    Aplica ``ST_MakeValid(ST_Buffer(geom, 0))``. Quando não é possível validar,
    devolve ``GEOMETRYCOLLECTION EMPTY`` se ``return_geometry`` for ``True``, ou
    ``NULL`` caso contrário. Requer ``SedonaContext`` ativa.
    """
    fixed = stf.ST_MakeValid(stf.ST_Buffer(col, 0))
    fallback = (
        stc.ST_GeomFromWKT(F.lit("GEOMETRYCOLLECTION EMPTY"))
        if return_geometry else F.lit(None)
    )
    return F.when(stf.ST_IsValid(fixed) & col.isNotNull(), fixed).otherwise(fallback)


def valid_geometry(col: Column) -> Column:
    """Devolve a geometria se válida; senão, repara com um buffer mínimo.

    Requer ``SedonaContext`` ativa.
    """
    return F.when(
        stf.ST_IsValid(col), col
    ).otherwise(stf.ST_MakeValid(stf.ST_Buffer(col, 0.0001)))


def st_intersection_area_safe(col_a: Column, col_b: Column) -> Column:
    """Área da interseção entre duas geometrias, ou ``0.0`` se inválida.

    Requer ``SedonaContext`` ativa.
    """
    intersection = stf.ST_Intersection(col_a, col_b)
    return F.when(
        stf.ST_IsValid(intersection), stf.ST_Area(intersection)
    ).otherwise(F.lit(0.0))


def degrees_to_meters(col: Column) -> Column:
    """Converte uma medida em graus para metros (aproximação ``* 0.11 / 1e-6``)."""
    return (col * 0.11) / 0.000001


# --- Janela ------------------------------------------------------------------

def forward_fill(col: Column) -> Column:
    """Preenche nulos com o último valor não nulo anterior (forward fill).

    Usa uma janela de todas as linhas anteriores até a atual. O chamador deve
    definir a ordenação (e, se preciso, o particionamento) da janela no DataFrame
    antes de aplicar.
    """
    window = Window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return F.last(col, ignorenulls=True).over(window)


# --- Códigos e chaves --------------------------------------------------------

def digits_only(col: Column) -> Column:
    """Mantém apenas os dígitos de um texto (remove tudo que não é ``[0-9]``)."""
    return F.regexp_replace(F.trim(col), r"[^0-9]", "")


def zero_pad_code(col: Column, width: int = 8) -> Column:
    """Normaliza um código numérico com zeros à esquerda até ``width`` posições.

    Extrai os dígitos e completa à esquerda com zeros, para casar a mesma chave
    aterrissada em formatos diferentes. Retorna ``NULL`` se não sobrar dígito.
    """
    digits = digits_only(col)
    return F.when(digits == "", None).otherwise(F.lpad(digits, width, "0"))


def surrogate_key(columns: list[str]) -> Column:
    """Chave substituta determinística: ``sha2`` da concatenação das colunas.

    Junta os valores com ``|`` (nulos viram string vazia) e aplica ``sha2`` de
    256 bits — útil para identificar a linha quando não há chave natural única.
    """
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns]
    return F.sha2(F.concat_ws("|", *parts), 256)


def standardize_uf(col: Column) -> Column:
    """Padroniza a sigla da UF: remove espaços e coloca em maiúsculas."""
    return F.upper(F.trim(col))


# --- Datas e tempo -----------------------------------------------------------

def parse_br_date(col: Column, fmt: str = "dd/MM/yyyy") -> Column:
    """Interpreta uma data em texto no formato brasileiro como ``date``."""
    return F.to_date(F.trim(col), fmt)


def parse_iso_timestamp(col: Column) -> Column:
    """Interpreta um ``timestamp`` ISO-8601 em texto como ``timestamp``."""
    return F.to_timestamp(F.trim(col))


def extract_year(col: Column) -> Column:
    """Extrai o ano (``int``) de uma coluna de data/``timestamp``."""
    return F.year(col)


def minutes_between(start_col: Column, end_col: Column) -> Column:
    """Minutos decorridos entre dois ``timestamp`` (``end`` - ``start``)."""
    return (end_col.cast("long") - start_col.cast("long")) / F.lit(60.0)


# --- Números e escalas -------------------------------------------------------

def money_br_to_double(col: Column) -> Column:
    """Converte moeda no formato brasileiro (``R$ 1.234,56``) em ``double``.

    Remove o símbolo de moeda e os separadores de milhar e troca a vírgula
    decimal por ponto antes do cast. Retorna ``NULL`` quando não é numérico.
    """
    no_symbol = F.regexp_replace(F.trim(col), r"[R$\s]", "")
    cleaned = F.regexp_replace(F.regexp_replace(no_symbol, r"\.", ""), ",", ".")
    return cleaned.cast("double")


def percent_to_fraction(col: Column) -> Column:
    """Converte um percentual em texto (``45,3%``) na fração equivalente (0,453)."""
    number = F.regexp_replace(F.regexp_replace(F.trim(col), "%", ""), ",", ".")
    return number.cast("double") / F.lit(100.0)


def coalesce_zero(col: Column) -> Column:
    """Substitui ``NULL`` por ``0`` mantendo o restante inalterado."""
    return F.coalesce(col, F.lit(0))


def null_if_negative(col: Column) -> Column:
    """Transforma valores negativos em ``NULL`` (sentinelas de ausência)."""
    return F.when(col < 0, F.lit(None)).otherwise(col)


def clamp(col: Column, lower: float, upper: float) -> Column:
    """Limita um valor numérico ao intervalo ``[lower, upper]``."""
    return F.greatest(F.lit(lower), F.least(F.lit(upper), col))


def safe_divide(numerator: Column, denominator: Column) -> Column:
    """Divide protegendo contra divisor zero/nulo (retorna ``NULL`` nesses casos)."""
    return F.when(
        denominator.isNull() | (denominator == 0), F.lit(None)
    ).otherwise(numerator / denominator)


def bucketize(col: Column, bounds: list[float], labels: list[str]) -> Column:
    """Mapeia um valor numérico em faixas rotuladas.

    ``bounds`` são os limites superiores (exclusivos) crescentes; ``labels`` tem
    um rótulo a mais que ``bounds`` (a última faixa é "acima do último limite").
    """
    result = F.when(col < F.lit(bounds[0]), F.lit(labels[0]))
    for i in range(1, len(bounds)):
        result = result.when(col < F.lit(bounds[i]), F.lit(labels[i]))
    return result.otherwise(F.lit(labels[len(bounds)]))


# --- Texto e categóricos -----------------------------------------------------

def strip_accents_lower(col: Column) -> Column:
    """Remove acentos/pontuação (via :func:`clear_string`) e passa a minúsculas."""
    return F.lower(clear_string(col))


def yes_no_to_int(col: Column) -> Column:
    """Converte marcadores de sim/não em ``1``/``0`` (``int``).

    Aceita ``S``/``N``, ``SIM``/``NAO``, ``TRUE``/``FALSE`` e ``1``/``0`` em
    qualquer caixa. Valores não reconhecidos viram ``NULL``.
    """
    upper = F.upper(F.trim(col))
    yes = upper.isin("S", "SIM", "TRUE", "1", "Y", "YES")
    no = upper.isin("N", "NAO", "NÃO", "FALSE", "0", "NO")
    return F.when(yes, F.lit(1)).when(no, F.lit(0)).otherwise(F.lit(None))
