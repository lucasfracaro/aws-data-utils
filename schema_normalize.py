"""schema_normalize - Normalizacao de DataFrames pandas e criacao de schemas Spark.

- Converter pandas -> Spark com tudo como texto (normalize_df) ou com tipos
  inferidos do pandas (pandas_to_spark_auto_schema + normalize_df_schema).
- Gerar schemas (todas as colunas string) a partir do cabecalho de um TXT no S3
  e ler o arquivo com esse schema.

Requer pyspark e uma SparkSession ativa (ou possivel de criar).
"""

from collections import Counter

import pandas as pd

from pyspark.sql.types import StructType, StructField, StringType

from utils_gerais import obter_spark


# ============================================================
# NORMALIZACAO E SCHEMA
# ============================================================

def normalize_value(x):
    """Normaliza um valor individual para envio ao Spark.

    Regras:
        - dict ou list -> texto JSON (json.dumps);
        - NaN (float) -> 0;
        - None, NaT, pd.NA -> None;
        - int, float, bool -> mantidos como estao;
        - qualquer outro tipo (Timestamp, Decimal, str...) -> str(valor).

    Parametros:
        x (Any): valor a normalizar.

    Retorna:
        Any: valor normalizado.

    Exemplo:
        >>> normalize_value({"a": 1}), normalize_value(float("nan")), normalize_value(None)
        ('{"a": 1}', 0, None)
    """
    import json
    import pandas as pd

    # Caso seja um dicionario ou lista, converte para JSON
    if isinstance(x, (dict, list)):
        return json.dumps(x)

    # Caso seja nulo (None ou NaN)
    elif pd.isnull(x):
        if isinstance(x, (int, float)):  # Numericos nulos
            return 0
        elif isinstance(x, str):  # Strings nulas
            return ""
        else:  # Outros tipos nulos
            return None

    # Caso seja numerico (int ou float), mantem o valor original
    elif isinstance(x, (int, float)):
        return x

    # Caso seja um objeto complexo, converte para string
    elif hasattr(x, '__str__'):
        return str(x)

    # Caso contrario, retorna o valor original convertido para string
    else:
        return str(x)


def _eh_nulo(v):
    """Retorna True para None, NaN, NaT e pd.NA, sem quebrar com listas/dicts (que nunca sao nulos).

    Parametros:
        v (Any): valor a verificar.

    Retorna:
        bool
    """
    if v is None:
        return True
    if isinstance(v, (list, dict, tuple, set)):
        return False
    try:
        return bool(pd.isna(v))
    except (TypeError, ValueError):
        return False


def _texto_ou_none(v):
    """Converte o valor para str, mantendo nulos (ver _eh_nulo) como None."""
    return None if _eh_nulo(v) else str(v)


def _serie_objeto(valores, index):
    """Cria uma pandas.Series com dtype object (impede que o pandas troque None por NaN/NaT)."""
    return pd.Series(valores, index=index, dtype=object)


def schema_stringtype(df):
    """Cria um StructType com todas as colunas do DataFrame como StringType (nullable).

    Parametros:
        df (pandas.DataFrame | pyspark.sql.DataFrame): qualquer objeto com o atributo ".columns".

    Retorna:
        pyspark.sql.types.StructType
    """
    column_names = df.columns
    # criar o schema automaticamente com StringType() para todas as colunas
    schema = StructType([StructField(col_name, StringType(), True) for col_name in column_names])
    return schema


def normalize_df(df):
    """Converte um DataFrame pandas em DataFrame Spark com TODAS as colunas como string.

    Cada valor passa por normalize_value e depois vira texto:
        - colunas numericas: NaN vira "0"; numeros viram texto ("1", "2.5");
        - colunas nao numericas: nulos viram None (null no Spark);
        - dict/list viram JSON.
    O DataFrame original nao e alterado.

    Parametros:
        df (pandas.DataFrame): dados de entrada.

    Retorna:
        pyspark.sql.DataFrame: todas as colunas StringType.

    Exemplo:
        >>> df_spark = normalize_df(pd.DataFrame({"id": [1, 2], "nome": ["a", None]}))
    """
    spark = obter_spark()

    # Normaliza todos os valores (em uma copia, para nao alterar o DataFrame de quem chamou).
    # Como o schema e todo StringType, os valores numericos precisam virar texto
    # (o Spark rejeita int/float em coluna StringType).
    df = df.copy()
    for col in df.columns:
        serie = df[col]
        if not pd.api.types.is_numeric_dtype(serie):
            # nulos em coluna de texto viram None (e nao 0, como aconteceria com NaN)
            serie = _serie_objeto([None if _eh_nulo(v) else v for v in serie], df.index)
        df[col] = _serie_objeto([_texto_ou_none(normalize_value(v)) for v in serie], df.index)

    df_spark = spark.createDataFrame(df, schema=schema_stringtype(df))
    return df_spark


def pandas_to_spark_auto_schema(pandas_df):
    """Gera um schema Spark (StructType) a partir dos dtypes de um DataFrame pandas.

    Mapeamento: int32->Integer, int64/Int64->Long, float32/float16->Float,
    float64->Double, bool->Boolean, datetime64[ns] (com ou sem UTC)->Timestamp,
    timedelta64[ns]->Long, UInt8->Short, UInt16->Integer, UInt32/UInt64->Long,
    object/category->String. Qualquer outro dtype vira String.

    Atencao: esta funcao NAO converte os dados, apenas monta o schema. Use com
    normalize_df_schema para criar o DataFrame Spark.

    Parametros:
        pandas_df (pandas.DataFrame): DataFrame cujos dtypes serao lidos.

    Retorna:
        pyspark.sql.types.StructType

    Exemplo:
        >>> schema = pandas_to_spark_auto_schema(df)
        >>> df_spark = normalize_df_schema(df, schema)
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, FloatType, DoubleType,
        LongType, ShortType, BooleanType, TimestampType
    )

    type_mapping = {
        'int32': IntegerType(),
        'int64': LongType(),
        'float32': FloatType(),
        'float64': DoubleType(),
        'float16': FloatType(),
        'object': StringType(),
        'bool': BooleanType(),
        'datetime64[ns]': TimestampType(),
        'datetime64[ns, UTC]': TimestampType(),
        'timedelta64[ns]': LongType(),
        'category': StringType(),
        'Int64': LongType(),
        'UInt8': ShortType(),
        'UInt16': IntegerType(),
        'UInt32': LongType(),
        'UInt64': LongType()
    }

    # Convertendo o schema do Pandas para PySpark
    spark_fields = []
    for column_name, dtype in pandas_df.dtypes.items():
        spark_type = type_mapping.get(str(dtype), StringType())  # Default para StringType se tipo for desconhecido
        spark_fields.append(StructField(column_name, spark_type, True))  # True indica que a coluna pode ter valores nulos

    # Encapsular os StructFields em um StructType
    spark_schema = StructType(spark_fields)
    return spark_schema


def normalize_df_schema(df, schema):
    """Converte um DataFrame pandas em DataFrame Spark aplicando o schema informado.

    Tratamento por tipo de coluna no schema:
        - Timestamp/Date: valores mantidos; NaT/NaN viram None;
        - String: normalize_value e depois str (nulos viram None);
        - demais tipos: normalize_value (NaN em coluna numerica vira 0).
    O DataFrame original nao e alterado.

    Parametros:
        df (pandas.DataFrame): dados de entrada.
        schema (pyspark.sql.types.StructType): schema de destino (ex.: gerado por
            pandas_to_spark_auto_schema). Os nomes das colunas devem coincidir.

    Retorna:
        pyspark.sql.DataFrame

    Levanta:
        ValueError: se o Spark rejeitar algum valor para o tipo definido no schema
            (a mensagem original do Spark vem junto).
    """
    from pyspark.sql.types import TimestampType, DateType

    tipos_por_coluna = {field.name: field.dataType for field in schema.fields}

    # Normaliza os valores em cada coluna do DataFrame Pandas (em uma copia)
    normalized_df = df.copy()
    for column in normalized_df.columns:
        tipo = tipos_por_coluna.get(column)
        serie = normalized_df[column]
        if isinstance(tipo, (TimestampType, DateType)):
            # normalize_value converteria datas para texto, o que o Spark rejeita
            # em coluna Timestamp/Date; aqui so troca NaT/NaN por None.
            valores = [None if _eh_nulo(v) else v for v in serie]
        elif isinstance(tipo, StringType):
            # coluna texto: normaliza e garante str (ou None)
            valores = [_texto_ou_none(normalize_value(None if _eh_nulo(v) else v)) for v in serie]
        else:
            valores = [normalize_value(v) for v in serie]
        # dtype=object: senao o pandas reconverte None em NaN/NaT
        normalized_df[column] = _serie_objeto(valores, normalized_df.index)

    spark = obter_spark()

    # Converte o DataFrame Pandas para PySpark utilizando o esquema fornecido
    try:
        df_spark = spark.createDataFrame(normalized_df, schema=schema)
    except Exception as e:
        raise ValueError(f"Erro ao converter o DataFrame Pandas para PySpark: {e}") from e

    return df_spark


def create_schema_from_s3_txt(s3_path, columns_comment=None, comments=None, delimiter="\t"):
    """Cria um schema Spark (todas as colunas string) a partir do cabecalho de um arquivo texto no S3.

    Os nomes das colunas sao limpos com normaliza_nome_coluna (remove BOM e
    espacos nas pontas). Nao valida colunas vazias nem duplicadas; para isso use
    create_schema_from_s3_txt_normalizado.

    Parametros:
        s3_path (str): caminho completo "s3://bucket/key" do arquivo de referencia
            (ou de uma pasta; o cabecalho e lido da primeira linha encontrada).
        columns_comment (list[str], opcional): colunas que devem receber comentario.
        comments (dict[str, str], opcional): {coluna: comentario}. So e usado para
            as colunas listadas em columns_comment (todas precisam existir aqui).
        delimiter (str, opcional): separador de colunas. Padrao: "\\t".

    Retorna:
        pyspark.sql.types.StructType

    Levanta:
        KeyError: se uma coluna de columns_comment nao estiver em comments.

    Exemplo:
        >>> schema = create_schema_from_s3_txt("s3://meu-bucket/dados/vendas_20260927.txt")
    """
    # Garantir que columns_comment e comments sejam inicializados corretamente
    columns_comment = columns_comment or []
    comments = comments or {}

    spark = obter_spark()
    rdd = spark.sparkContext.textFile(s3_path)
    header = rdd.first()
    column_names = [normaliza_nome_coluna(c) for c in header.split(delimiter)]

    schema = StructType([
        StructField(
            col_name,
            StringType(),
            True,
            {"comment": comments[col_name]} if col_name in columns_comment else {}
        )
        for col_name in column_names
    ])

    return schema


def read_txt_to_df_with_schema(s3_path, schema, delimiter="\t"):
    """Le um arquivo texto do S3 com o schema informado, dividindo cada linha pelo separador (via RDD).

    Comportamento:
        - todas as linhas iguais ao cabecalho sao descartadas (inclusive
          cabecalhos repetidos quando s3_path e uma pasta com varios arquivos);
        - linhas com menos colunas que o schema sao completadas com None;
        - linhas com colunas a mais: o excedente fica na ultima coluna;
        - nao trata aspas; para arquivos com campos entre aspas prefira
          read_txt_to_df_with_schema_normalizado (leitor CSV do Spark).

    Parametros:
        s3_path (str): caminho "s3://bucket/key" (arquivo ou pasta).
        schema (pyspark.sql.types.StructType): schema de leitura (ex.: de create_schema_from_s3_txt).
        delimiter (str, opcional): separador de colunas. Padrao: "\\t".

    Retorna:
        pyspark.sql.DataFrame
    """
    spark = obter_spark()
    rdd = spark.sparkContext.textFile(s3_path)

    # Obter a primeira linha como cabecalho
    header = rdd.first()

    qtd_colunas = len(schema.fields)

    def _ajustar_linha(row):
        # split com limite evita linhas com colunas "a mais"; linhas com colunas
        # "a menos" (ex.: campos vazios no final) sao completadas com None,
        # senao o createDataFrame falha com "Length of object does not match".
        """Divide a linha em exatamente len(schema) colunas (completa com None ou junta o excedente na ultima)."""
        valores = row.split(delimiter, qtd_colunas - 1)
        if len(valores) < qtd_colunas:
            valores += [None] * (qtd_colunas - len(valores))
        return valores

    # Criar um RDD ignorando a primeira linha (cabecalho)
    data_rdd = rdd.filter(lambda row: row != header).map(_ajustar_linha)

    # Criar o DataFrame com o schema especificado
    df = spark.createDataFrame(data_rdd, schema=schema)

    return df


def normaliza_nome_coluna(nome_coluna):
    """Limpa o nome de uma coluna do cabecalho: remove o BOM (\\ufeff) e os espacos nas pontas.

    Nao altera o restante do nome (acentos e simbolos como ">" sao mantidos, porque
    o DDL e gerado com crases).

    Parametros:
        nome_coluna (str | None): nome original.

    Retorna:
        str | None: nome limpo (None continua None).
    """
    if nome_coluna is None:
        return nome_coluna

    nome_coluna = nome_coluna.replace("\ufeff", "")
    nome_coluna = nome_coluna.strip()

    return nome_coluna


def create_schema_from_s3_txt_normalizado(s3_path, columns_comment=None, comments=None, delimiter="\t"):
    """Cria um schema Spark (todas as colunas string) a partir do cabecalho de um TXT no S3, validando o cabecalho.

    Igual a create_schema_from_s3_txt, mas falha cedo se o cabecalho tiver
    problemas que quebrariam a tabela no Athena.

    Parametros:
        s3_path (str): caminho completo "s3://bucket/key" do arquivo de referencia
            (ou de uma pasta; o cabecalho e lido da primeira linha encontrada).
        columns_comment (list[str], opcional): colunas que devem receber comentario.
        comments (dict[str, str], opcional): {coluna: comentario}. So e usado para
            as colunas listadas em columns_comment (todas precisam existir aqui).
        delimiter (str, opcional): separador de colunas. Padrao: "\\t".

    Retorna:
        pyspark.sql.types.StructType

    Levanta:
        ValueError: cabecalho vazio, com colunas sem nome ou com nomes duplicados.
        KeyError: se uma coluna de columns_comment nao estiver em comments.
    """
    columns_comment = columns_comment or []
    comments = comments or {}

    spark = obter_spark()
    rdd = spark.sparkContext.textFile(s3_path)
    header = rdd.first()

    if header is None or header.strip() == "":
        raise ValueError(f"O arquivo nao possui header valido: {s3_path}")

    column_names = [normaliza_nome_coluna(col_name) for col_name in header.split(delimiter)]

    # Verificar colunas vazias
    colunas_vazias = [name for name in column_names if name == ""]
    if colunas_vazias:
        raise ValueError(
            f"Foram encontradas colunas vazias no header. "
            f"Arquivo: {s3_path}"
        )

    # Verificar colunas duplicadas
    duplicadas = [name for name, count in Counter(column_names).items() if count > 1]
    if duplicadas:
        raise ValueError(
            f"Foram encontradas colunas duplicadas no header: {duplicadas}. "
            f"Arquivo: {s3_path}"
        )

    schema = StructType([
        StructField(
            col_name,
            StringType(),
            True,
            {"comment": comments[col_name]} if col_name in columns_comment else {}
        )
        for col_name in column_names
    ])

    return schema


def read_txt_to_df_with_schema_normalizado(s3_path, schema, delimiter="\t"):
    """Le um TXT/CSV do S3 com o leitor CSV do Spark usando o schema informado (cabecalho na 1a linha, UTF-8).

    Diferente de read_txt_to_df_with_schema, respeita campos entre aspas e usa o
    cabecalho de cada arquivo (header=True), sem inferir tipos.

    Parametros:
        s3_path (str): caminho "s3://bucket/key" (arquivo ou pasta).
        schema (pyspark.sql.types.StructType): schema de leitura, na mesma ordem das colunas do arquivo.
        delimiter (str, opcional): separador de colunas. Padrao: "\\t".

    Retorna:
        pyspark.sql.DataFrame

    Exemplo:
        >>> caminho = "s3://meu-bucket/dados/vendas_20260927.txt"
        >>> df = read_txt_to_df_with_schema_normalizado(caminho, create_schema_from_s3_txt_normalizado(caminho))
    """
    df = (
        obter_spark()
        .read
        .option("delimiter", delimiter)
        .option("encoding", "UTF-8")
        .option("header", True)
        .option("inferSchema", False)
        .schema(schema)
        .format("csv")
        .load(s3_path)
    )

    return df
