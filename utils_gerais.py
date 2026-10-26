"""utils_gerais - Funcoes utilitarias compartilhadas pelos demais modulos.

Contexto Spark/Glue/logger obtido sob demanda, verificacao de valores vazios,
normalizacao de valores de particao, impressao de tabelas no terminal e criacao
de arquivos vazios (zip/Excel).
"""

import logging
from typing import Dict, Any

import pandas as pd


# ============================================================
# 1. CONTEXTO SPARK / GLUE / LOGGER (compartilhado entre os modulos)
# ============================================================
# Os modulos nao dependem de variaveis globais "spark", "glueContext" ou
# "logger": as funcoes abaixo obtem (ou criam) essas instancias sob demanda,
# reaproveitando a sessao ja existente no job.

_glue_context = None


def obter_spark():
    """Retorna a SparkSession ativa ou cria uma nova, se ainda nao existir.

    Usada pelos demais modulos no lugar de uma variavel global "spark", para que
    funcionem tanto num job Glue (reaproveitando a sessao do job) quanto em
    scripts/notebooks.

    Retorna:
        pyspark.sql.SparkSession
    """
    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()


def obter_glue_context():
    """Retorna um GlueContext criado sobre o SparkContext ativo (reutilizado entre chamadas).

    Requer o pacote "awsglue", disponivel apenas em jobs AWS Glue ou em imagens
    locais do Glue.

    Retorna:
        awsglue.context.GlueContext

    Levanta:
        ImportError: se "awsglue" nao estiver instalado.
    """
    global _glue_context
    if _glue_context is None:
        from pyspark import SparkContext
        from awsglue.context import GlueContext
        _glue_context = GlueContext(SparkContext.getOrCreate())
    return _glue_context


NOME_LOGGER_PADRAO = "utils_aws"


def obter_logger(nome=None):
    """Retorna o logger compartilhado pelos modulos.

    Os modulos registram mensagens com este logger. Para ver os logs, configure
    um handler nele no inicio do job ou script.

    Parametros:
        nome (str, opcional): nome do logger. Padrao: "utils_aws" (NOME_LOGGER_PADRAO).

    Retorna:
        logging.Logger

    Exemplo:
        >>> import logging
        >>> logging.basicConfig(level=logging.INFO)
        >>> obter_logger().info("inicio do processamento")
    """
    return logging.getLogger(nome or NOME_LOGGER_PADRAO)


# ============================================================
# 2. UTILITARIOS GERAIS
# ============================================================

def valor_vazio(valor) -> bool:
    """Verifica se um valor deve ser tratado como vazio.

    Considera vazio: None, NaN/NaT/pd.NA e textos que, sem espacos e em
    minusculas, sejam "", "nan", "none", "null" ou "<na>".

    Parametros:
        valor (Any): valor a verificar.

    Retorna:
        bool: True se o valor e vazio.

    Exemplo:
        >>> valor_vazio("  NULL "), valor_vazio(0), valor_vazio(float("nan"))
        (True, False, True)
    """
    if valor is None:
        return True
    try:
        if pd.isna(valor):
            return True
    except Exception:
        pass
    texto = str(valor).strip().lower()
    return texto in {"", "nan", "none", "null", "<na>"}


def normalizar_valor_particao(valor):
    """Converte um valor para o texto usado em particoes Hive (caminho S3 e ALTER TABLE).

    Regras:
        - None/NaN/NaT -> None
        - pd.Timestamp -> "YYYY-MM-DD"
        - float inteiro (ex.: 202401.0) -> "202401"
        - demais valores -> str(valor) sem espacos nas pontas

    Parametros:
        valor (Any): valor da coluna de particao.

    Retorna:
        str | None: valor normalizado, ou None se o valor for nulo.

    Exemplo:
        >>> normalizar_valor_particao(20240131.0)
        '20240131'
    """
    if valor is None:
        return None
    try:
        if pd.isna(valor):
            return None
    except (TypeError, ValueError):
        # listas/dicts/arrays: pd.isna devolve um array e nao um bool
        pass
    if isinstance(valor, pd.Timestamp):
        return valor.strftime("%Y-%m-%d")
    if isinstance(valor, float) and valor.is_integer():
        return str(int(valor))
    return str(valor).strip()


def dicionario_contido(filtro: Dict[str, Any], dados: Dict[str, Any]) -> bool:
    """Verifica se todos os pares chave/valor de "filtro" existem, iguais, em "dados".

    Parametros:
        filtro (dict): pares chave/valor que precisam estar presentes.
        dados (dict): dicionario onde os pares serao procurados. Se nao for dict,
            a funcao retorna False.

    Retorna:
        bool: True se todos os pares de "filtro" estao em "dados".

    Levanta:
        ValueError: se "filtro" nao for um dicionario.

    Exemplo:
        >>> dicionario_contido({"a": 1}, {"a": 1, "b": 2})
        True
    """
    if not isinstance(filtro, dict):
        raise ValueError("O parametro filtro deve ser um dicionario.")
    if not isinstance(dados, dict):
        return False
    for chave, valor in filtro.items():
        if dados.get(chave) != valor:
            return False
    return True


def _formatar_numero(valor, casas_decimais=3, limite_digitos_inteiros=20, casas_cientifica=20):
    """Formata um numero para exibicao, sem notacao cientifica ate "limite_digitos_inteiros" digitos inteiros.

    Regras:
        - inteiros: numero completo (ex.: 123456789012345678);
        - decimais: arredondados (meio para cima) em ate "casas_decimais" casas,
          sem zeros sobrando no final (ex.: 1234.5 -> "1234.5"; 0.12345 -> "0.123");
        - mais de "limite_digitos_inteiros" digitos na parte inteira: notacao
          cientifica com ate "casas_cientifica" casas (ex.: "1.23456789e+25");
        - None/NaN/NaT/pd.NA -> "" (celula vazia); +inf/-inf -> "inf"/"-inf";
        - bool e valores nao numericos -> str(valor).

    Parametros:
        valor (Any): valor a formatar.
        casas_decimais (int, opcional): maximo de casas decimais fora da notacao cientifica. Padrao: 3.
        limite_digitos_inteiros (int, opcional): acima desta quantidade de digitos
            inteiros usa notacao cientifica. Padrao: 20.
        casas_cientifica (int, opcional): maximo de casas decimais na notacao cientifica. Padrao: 20.

    Retorna:
        str

    Exemplo:
        >>> _formatar_numero(12345678901234567.891), _formatar_numero(2.5), _formatar_numero(10**25)
        ('12345678901234568', '2.5', '1e+25')
    """
    import math
    import numbers
    from decimal import Decimal, ROUND_HALF_UP, localcontext

    if valor is None or isinstance(valor, (bool, str)):
        return "" if valor is None else str(valor)
    try:
        if pd.isna(valor):
            return ""
    except (TypeError, ValueError):
        return str(valor)
    if not isinstance(valor, (numbers.Number, Decimal)) or isinstance(valor, complex):
        return str(valor)

    if isinstance(valor, numbers.Integral):
        # int do Python e inteiros do numpy: exatos, sem passar por float
        numero = Decimal(int(valor))
    elif isinstance(valor, Decimal):
        numero = valor
    else:
        valor = float(valor)
        if math.isinf(valor):
            return "inf" if valor > 0 else "-inf"
        # repr() usa a menor representacao que identifica o float (evita 0.1 -> 0.1000000000000000055...)
        numero = Decimal(repr(valor))

    if numero.is_nan():
        return ""
    if numero.is_infinite():
        return "inf" if numero > 0 else "-inf"

    # adjusted() = expoente do digito mais significativo (funciona tambem para 1.5E+25)
    digitos_inteiros = abs(numero).adjusted() + 1 if abs(numero) >= 1 else 1

    if digitos_inteiros > limite_digitos_inteiros:
        texto = f"{numero:.{casas_cientifica}e}"
        mantissa, expoente = texto.split("e")
        if "." in mantissa:
            mantissa = mantissa.rstrip("0").rstrip(".")
        return f"{mantissa}e{expoente}"

    with localcontext() as ctx:
        ctx.prec = limite_digitos_inteiros + casas_decimais + 10
        arredondado = numero.quantize(Decimal(1).scaleb(-casas_decimais), rounding=ROUND_HALF_UP)
    texto = f"{arredondado:f}"
    if "." in texto:
        texto = texto.rstrip("0").rstrip(".")
    if texto in ("-0", ""):
        texto = "0"
    return texto


def print_df(df, num_linhas=360):
    """Imprime um pandas.DataFrame como tabela no terminal (formato "psql" do tabulate).

    Formatacao dos numeros (ver _formatar_numero):
        - sem notacao cientifica: o numero e mostrado completo
          (ex.: 1000000000000000 e nao 1e+15);
        - no maximo 3 casas decimais, arredondadas (meio para cima) e sem zeros
          sobrando (ex.: 2.5, 0.123, 1234.5);
        - notacao cientifica apenas acima de 20 digitos na parte inteira, com ate
          20 casas decimais (ex.: 1.2345678901234568e+22);
        - valores nulos aparecem como celula vazia.
    Floats tem ~15-17 digitos significativos: 1234567890123456.78 e armazenado
    como 1234567890123456.8 e e exibido assim. Para valores exatos, use Decimal.

    Colunas numericas (inclusive colunas object so com Decimal/int/float) ficam
    alinhadas a direita. O DataFrame original nao e alterado.

    Parametros:
        df (pandas.DataFrame): DataFrame a imprimir.
        num_linhas (int, opcional): quantidade maxima de linhas exibidas. Padrao: 360.

    Retorna:
        None. Imprime "DataFrame vazio" se nao houver linhas.

    Requer:
        tabulate (pip install tabulate).

    Exemplo:
        >>> print_df(pd.DataFrame({"id": [1, 2], "valor": [1e15, 0.12345]}))
        +------+------------------+
        |   id |            valor |
        |------+------------------|
        |    1 | 1000000000000000 |
        |    2 |            0.123 |
        +------+------------------+
        <BLANKLINE>
        <BLANKLINE>
    """
    import pandas as pd
    from tabulate import tabulate

    if df.empty:
        print("DataFrame vazio\n")
        return

    import numbers
    from decimal import Decimal

    def _coluna_numerica(serie):
        if pd.api.types.is_bool_dtype(serie):
            return False
        if pd.api.types.is_numeric_dtype(serie):
            return True
        # colunas object com Decimal/int/float (ex.: vindas de banco de dados)
        valores = serie.dropna()
        return len(valores) > 0 and all(
            isinstance(v, (numbers.Number, Decimal)) and not isinstance(v, bool) for v in valores
        )

    df_to_print = df.head(num_linhas)
    colunas_numericas = [_coluna_numerica(df_to_print.iloc[:, i]) for i in range(df_to_print.shape[1])]
    linhas = [[_formatar_numero(v) for v in linha] for linha in df_to_print.itertuples(index=False, name=None)]
    # disable_numparse: senao o tabulate reconverte os textos numericos e volta a
    # usar notacao cientifica; colunas numericas continuam alinhadas a direita
    print(tabulate(
        linhas,
        headers=[str(c) for c in df_to_print.columns],
        tablefmt="psql",
        disable_numparse=True,
        colalign=["right" if num else "left" for num in colunas_numericas],
    ))
    print("\n")


def print_lista(lista, nome_coluna="VALOR", num_items=360):
    """Imprime uma lista (ou qualquer iteravel) como tabela no terminal, com a coluna de posicao "POS".

    Os numeros seguem a mesma formatacao de print_df:
    Formatacao dos numeros (ver _formatar_numero):
        - sem notacao cientifica: o numero e mostrado completo
          (ex.: 1000000000000000 e nao 1e+15);
        - no maximo 3 casas decimais, arredondadas (meio para cima) e sem zeros
          sobrando (ex.: 2.5, 0.123, 1234.5);
        - notacao cientifica apenas acima de 20 digitos na parte inteira, com ate
          20 casas decimais (ex.: 1.2345678901234568e+22);
        - valores nulos aparecem como celula vazia.
    Floats tem ~15-17 digitos significativos: 1234567890123456.78 e armazenado
    como 1234567890123456.8 e e exibido assim. Para valores exatos, use Decimal.

    Parametros:
        lista (iterable): valores a exibir (lista, pandas.Index, set etc.).
        nome_coluna (str, opcional): titulo da coluna de valores. Padrao: "VALOR".
        num_items (int, opcional): quantidade maxima de itens exibidos. Padrao: 360.

    Retorna:
        None. Imprime "Lista vazia" se nao houver itens.

    Requer:
        tabulate (pip install tabulate).

    Exemplo:
        >>> print_lista(df.columns, nome_coluna="COLUNA")
    """
    from tabulate import tabulate

    itens = list(lista)[:num_items]
    if not itens:
        print("Lista vazia\n")
        return
    print(tabulate(
        [[pos, _formatar_numero(v)] for pos, v in enumerate(itens, start=1)],
        headers=["POS", nome_coluna],
        tablefmt="psql",
        disable_numparse=True,
        colalign=["right", "left"],
    ))
    print("\n")


def ler_csv_com_fallback_encoding(caminho, encodings=None, **kwargs):
    """Le um CSV tentando varios encodings, na ordem, ate um funcionar.

    Parametros:
        caminho (str): caminho do arquivo CSV.
        encodings (list[str], opcional): encodings a tentar.
            Padrao: ["utf-8", "latin-1", "cp1252"].
        **kwargs: argumentos repassados para pandas.read_csv (sep, dtype, usecols...).

    Retorna:
        pandas.DataFrame

    Levanta:
        ValueError: se nenhum encoding conseguir ler o arquivo.

    Observacao:
        "latin-1" aceita qualquer sequencia de bytes, entao, se estiver na lista,
        a leitura nunca falha por encoding: os encodings seguintes nunca serao tentados.

    Exemplo:
        >>> df = ler_csv_com_fallback_encoding("dados.csv", sep=";", dtype=str)
    """
    import pandas as pd

    if encodings is None:
        encodings = ['utf-8', 'latin-1', 'cp1252']

    for encoding in encodings:
        try:
            return pd.read_csv(caminho, encoding=encoding, **kwargs)
        except (UnicodeDecodeError, UnicodeError):
            continue

    raise ValueError(f"Nao foi possivel ler o arquivo {caminho} com nenhum dos encodings: {encodings}")


def criar_texto_zerado_zip(caminho_zip, nome_arquivo_texto="arquivo.txt", conteudo=""):
    """Cria um arquivo ZIP contendo um unico arquivo de texto (vazio ou com o conteudo informado).

    A pasta do ZIP e criada, se nao existir. Um ZIP existente no mesmo caminho e sobrescrito.

    Parametros:
        caminho_zip (str): caminho do .zip a criar.
        nome_arquivo_texto (str, opcional): nome do arquivo dentro do ZIP. Padrao: "arquivo.txt".
        conteudo (str, opcional): texto do arquivo. Padrao: "" (vazio).

    Retorna:
        None

    Exemplo:
        >>> criar_texto_zerado_zip("saida/vazio.zip", "vendas.txt", "col_a;col_b\\n")
    """
    import zipfile
    import os

    pasta = os.path.dirname(caminho_zip)
    if pasta:  # os.makedirs("") gera FileNotFoundError quando o caminho nao tem pasta
        os.makedirs(pasta, exist_ok=True)

    with zipfile.ZipFile(caminho_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(nome_arquivo_texto, conteudo)


def criar_excel_zerado_fisico(caminho_excel, nome_aba="Sheet1"):
    """Cria um arquivo Excel (.xlsx) sem dados, com uma unica aba.

    A pasta do arquivo e criada, se nao existir. Um arquivo existente no mesmo caminho e sobrescrito.

    Parametros:
        caminho_excel (str): caminho do .xlsx a criar.
        nome_aba (str, opcional): nome da aba. Padrao: "Sheet1".

    Retorna:
        None

    Requer:
        openpyxl.
    """
    import pandas as pd
    import os

    pasta = os.path.dirname(caminho_excel)
    if pasta:  # os.makedirs("") gera FileNotFoundError quando o caminho nao tem pasta
        os.makedirs(pasta, exist_ok=True)

    df_vazio = pd.DataFrame()
    with pd.ExcelWriter(caminho_excel, engine='openpyxl') as writer:
        df_vazio.to_excel(writer, sheet_name=nome_aba, index=False)
