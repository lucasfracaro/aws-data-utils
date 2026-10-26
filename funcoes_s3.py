"""funcoes_s3 - Operacoes no Amazon S3: verificacao de arquivos, escrita de tabelas
Spark como TXT unico com cabecalho, leitura de arquivos e limpeza de prefixos.

As funcoes de escrita/leitura via Spark usam a SparkSession ativa
(utils_gerais.obter_spark). As demais usam apenas boto3, com as credenciais
e regiao da configuracao padrao.
"""

import boto3
from botocore.exceptions import ClientError

from utils_gerais import obter_spark


def _encontrar_arquivo_part(response):
    """Retorna a key do arquivo de dados ("part-*") de uma resposta list_objects_v2, ignorando _SUCCESS e .crc.

    Parametros:
        response (dict): resposta de s3_client.list_objects_v2.

    Retorna:
        str | None: a primeira key "part-*" encontrada, ou None.
    """
    for obj in response.get("Contents", []):
        nome = obj["Key"].rsplit("/", 1)[-1]
        if nome.startswith("part-") and not nome.endswith(".crc"):
            return obj["Key"]
    return None


# ============================================================
# VERIFICACAO E RESOLUCAO DE DADOS
# ============================================================

def arquivo_existe(bucket, key):
    """Verifica se um objeto existe no S3 (via head_object).

    Parametros:
        bucket (str): nome do bucket.
        key (str): caminho completo do objeto dentro do bucket (sem "s3://bucket/").

    Retorna:
        bool: True se o objeto existe, False se o S3 responder 404.

    Levanta:
        botocore.exceptions.ClientError: para qualquer outro erro (ex.: 403). Sem a
            permissao s3:ListBucket, o S3 responde 403 (e nao 404) para objetos
            inexistentes, e a excecao e relancada.

    Exemplo:
        >>> arquivo_existe("meu-bucket", "dados/vendas_20260927.txt")
    """
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise


def resolver_data_anterior(bucket, data_d1, data_d2, data_d3, tabela_destino, prefixo):
    """Escolhe a data do processamento anterior que existe no S3, para calcular deltas.

    Procura, no prefixo informado, os arquivos "<tabela_destino>_<data>.txt":
        1. se existir o de data_d2 -> retorna data_d2;
        2. senao, se existir o de data_d3 -> retorna data_d3;
        3. senao -> retorna data_d1 (o processamento sera comparado com ele mesmo,
           sem gerar delta).

    Parametros:
        bucket (str): nome do bucket.
        data_d1 (str): data do processamento atual, "YYYYMMDD".
        data_d2 (str): data do processamento anterior, "YYYYMMDD".
        data_d3 (str): data do processamento de dois dias atras, "YYYYMMDD".
        tabela_destino (str): nome base do arquivo, sem a data (ex.: "vendas").
        prefixo (str): "pasta" dentro do bucket (com ou sem "/" no final).

    Retorna:
        str: a data escolhida, "YYYYMMDD".

    Exemplo:
        >>> from datas import avaliar_dia_diario
        >>> _, d1, d2, d3, *_ = avaliar_dia_diario()
        >>> data_comparacao = resolver_data_anterior("meu-bucket", d1, d2, d3, "vendas", "historico")
    """
    if prefixo and not prefixo.endswith('/'):
        prefixo += '/'

    key1 = f'{prefixo}{tabela_destino}_{data_d2}.txt'
    key2 = f'{prefixo}{tabela_destino}_{data_d3}.txt'

    # print(f"verificando arquivo: {key1}")
    # print(f"verificando arquivo: {key2}")

    if arquivo_existe(bucket, key1):
        return data_d2
    elif arquivo_existe(bucket, key2):
        return data_d3
    else:
        return data_d1


# ============================================================
# ESCRITA NO S3
# ============================================================

def save_txt_historico(spark_table, bucket_name, temp_output_prefix, destination_key):
    """Grava uma tabela Spark como um unico arquivo TXT (separado por TAB, com cabecalho) em um caminho fixo do S3.

    Funcionamento:
        1. Le a tabela/view "spark_table" do catalogo Spark.
        2. Troca nulos por "" e junta as colunas com TAB em uma unica coluna de texto.
        3. Grava cabecalho + dados em UMA particao (um unico arquivo part-*) em
           s3://<bucket_name>/<temp_output_prefix>/.
        4. Copia esse arquivo para "destination_key" e apaga o prefixo temporario inteiro.

    Atencao:
        - Valores que contenham TAB ou quebra de linha nao sao escapados e vao
          quebrar a estrutura do arquivo.
        - coalesce(1) leva todos os dados para um unico executor: adequado para
          volumes moderados.
        - Use um temp_output_prefix exclusivo: todo o conteudo dele e apagado.

    Parametros:
        spark_table (str): nome da tabela ou view temporaria registrada no Spark.
        bucket_name (str): bucket de destino.
        temp_output_prefix (str): prefixo temporario usado pelo Spark (sera apagado).
        destination_key (str): key final do arquivo (ex.: "historico/vendas_20260927.txt").

    Retorna:
        None. Imprime um aviso se o Spark nao gerar nenhum arquivo.

    Exemplo:
        >>> df.createOrReplaceTempView("vendas")
        >>> save_txt_historico("vendas", "meu-bucket", "tmp/vendas", "historico/vendas_20260927.txt")
    """
    import boto3
    from pyspark.sql.functions import coalesce, col, lit, concat_ws

    spark = obter_spark()

    # Cria DataFrame a partir da tabela
    spark_df = spark.table(spark_table)

    # Obter os nomes das colunas
    col_names = spark_df.columns

    # Criar uma string com os nomes das colunas separados por tabulacao
    header_str = "\t".join(col_names)

    # Criar um DataFrame para os nomes das colunas com uma coluna de ordenacao
    header_df = spark.createDataFrame([(header_str, 0)], "linha string, orden int")

    # Substituir valores nulos por strings vazias em todas as colunas
    spark_df = spark_df.select([coalesce(col(c), lit("")).alias(c) for c in spark_df.columns])

    # Transformar o DataFrame original em uma unica coluna com os valores concatenados
    data_df = spark_df.withColumn("linha", concat_ws("\t", *spark_df.columns)).select("linha")

    # Adicionar uma coluna de ordenacao ao DataFrame de dados
    data_df = data_df.withColumn("orden", lit(1))

    # Unir o cabecalho com os dados
    final_df = header_df.union(data_df)

    # Ordenar pelo campo de ordenacao para garantir que o cabecalho esteja no inicio
    final_df = final_df.orderBy("orden")

    # Reduzir para uma unica particao e salvar no S3
    final_df.select("linha").coalesce(1).write.mode("overwrite").text(f"s3://{bucket_name}/{temp_output_prefix}")

    # Renomear o arquivo no S3
    s3 = boto3.client('s3')
    # o Spark grava dentro da "pasta" temp_output_prefix/; usar a barra no final evita
    # casar (e depois apagar) outros objetos que apenas comecem com o mesmo texto
    prefixo_temp = temp_output_prefix.strip("/") + "/"
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefixo_temp)

    # Encontrar o arquivo de dados gerado (part-*), ignorando o _SUCCESS vazio
    source_key = _encontrar_arquivo_part(response)

    if source_key:
        # Copiar o arquivo com o novo nome
        s3.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': source_key},
            Key=destination_key
        )
        # Excluir o prefixo temporario inteiro (part-*, _SUCCESS etc.)
        apagar_prefixo_s3_client(s3, bucket_name, prefixo_temp)
    else:
        print("Nenhum arquivo temporario encontrado para renomear.")


def save_txt_substituindo_anterior(spark_table, bucket_name, temp_output_prefix, destination_key, previous_key):
    """Igual a save_txt_historico, mas depois apaga o arquivo da carga anterior (mantendo so a versao atual).

    Funcionamento:
        1. Le a tabela/view "spark_table" do catalogo Spark.
        2. Troca nulos por "" e junta as colunas com TAB em uma unica coluna de texto.
        3. Grava cabecalho + dados em UMA particao (um unico arquivo part-*) em
           s3://<bucket_name>/<temp_output_prefix>/.
        4. Copia esse arquivo para "destination_key" e apaga o prefixo temporario inteiro.

    Atencao:
        - Valores que contenham TAB ou quebra de linha nao sao escapados e vao
          quebrar a estrutura do arquivo.
        - coalesce(1) leva todos os dados para um unico executor: adequado para
          volumes moderados.
        - Use um temp_output_prefix exclusivo: todo o conteudo dele e apagado.
        5. Apaga "previous_key", se for diferente de destination_key. Se nenhum
           arquivo novo for gerado, o anterior NAO e apagado.

    Parametros:
        spark_table (str): nome da tabela ou view temporaria registrada no Spark.
        bucket_name (str): bucket de destino.
        temp_output_prefix (str): prefixo temporario usado pelo Spark (sera apagado).
        destination_key (str): key final do novo arquivo.
        previous_key (str | None): key do arquivo da carga anterior, a remover.

    Retorna:
        None

    Levanta:
        botocore.exceptions.ClientError: em erros ao apagar o arquivo anterior
            (exceto NoSuchKey).

    Exemplo:
        >>> save_txt_substituindo_anterior("vendas", "meu-bucket", "tmp/vendas",
        ...                "atual/vendas_20260927.txt", "atual/vendas_20260926.txt")
    """
    import boto3
    from pyspark.sql.functions import coalesce, col, lit, concat_ws

    spark = obter_spark()

    # Cria DataFrame a partir da tabela
    spark_df = spark.table(spark_table)

    # Obter os nomes das colunas
    col_names = spark_df.columns

    # Criar uma string com os nomes das colunas separados por tabulacao
    header_str = "\t".join(col_names)

    # Criar um DataFrame para os nomes das colunas com uma coluna de ordenacao
    header_df = spark.createDataFrame([(header_str, 0)], "linha string, orden int")

    # Substituir valores nulos por strings vazias em todas as colunas
    spark_df = spark_df.select([coalesce(col(c), lit("")).alias(c) for c in spark_df.columns])

    # Transformar o DataFrame original em uma unica coluna com os valores concatenados
    data_df = spark_df.withColumn("linha", concat_ws("\t", *spark_df.columns)).select("linha")

    # Adicionar uma coluna de ordenacao ao DataFrame de dados
    data_df = data_df.withColumn("orden", lit(1))

    # Unir o cabecalho com os dados
    final_df = header_df.union(data_df)

    # Ordenar pelo campo de ordenacao para garantir que o cabecalho esteja no inicio
    final_df = final_df.orderBy("orden")

    # Reduzir para uma unica particao e salvar no S3
    final_df.select("linha").coalesce(1).write.mode("overwrite").text(f"s3://{bucket_name}/{temp_output_prefix}")

    # Renomear o arquivo no S3
    s3 = boto3.client('s3')
    # o Spark grava dentro da "pasta" temp_output_prefix/; usar a barra no final evita
    # casar (e depois apagar) outros objetos que apenas comecem com o mesmo texto
    prefixo_temp = temp_output_prefix.strip("/") + "/"
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefixo_temp)

    # Encontrar o arquivo de dados gerado (part-*), ignorando o _SUCCESS vazio
    source_key = _encontrar_arquivo_part(response)

    if source_key:
        # Copiar o arquivo com o novo nome
        s3.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': source_key},
            Key=destination_key
        )
        # Excluir o prefixo temporario inteiro (part-*, _SUCCESS etc.)
        apagar_prefixo_s3_client(s3, bucket_name, prefixo_temp)
    else:
        # sem arquivo novo, NAO apaga o anterior (senao a carga ficaria sem nenhum arquivo)
        print("Nenhum arquivo temporario encontrado para renomear. Arquivo anterior mantido.")
        return

    # Tentar deletar o arquivo do processamento anterior
    if previous_key and destination_key != previous_key:
        try:
            s3.delete_object(Bucket=bucket_name, Key=previous_key)
            print(f"Arquivo {previous_key} deletado com sucesso.")
        except ClientError as e:
            if e.response["Error"]["Code"] == 'NoSuchKey':
                print(f"Arquivo {previous_key} nao encontrado. Nenhuma acao necessaria.")
            else:
                raise  # Re-levantar outras excecoes


def mover_para_quarentena(bucket_name, del_key, error_key):
    """Move um arquivo para outro caminho do mesmo bucket (copia e depois apaga o original).

    Util para tirar do historico um arquivo com problema, guardando-o em uma pasta de erros.

    Parametros:
        bucket_name (str): bucket onde estao os arquivos.
        del_key (str): key do arquivo original (sera apagado apos a copia).
        error_key (str): key de destino da copia.

    Retorna:
        None. Se o original nao existir, apenas imprime um aviso.

    Levanta:
        botocore.exceptions.ClientError: em erros diferentes de NoSuchKey.

    Exemplo:
        >>> mover_para_quarentena("meu-bucket", "historico/vendas_20260927.txt",
        ...                          "erros/vendas_20260927.txt")
    """
    import boto3
    from botocore.exceptions import ClientError

    s3 = boto3.client('s3')

    try:
        s3.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': del_key},
            Key=error_key
        )
        print(f"Arquivo {del_key} copiado com sucesso para {error_key}.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"Arquivo {del_key} nao encontrado. Nao foi possivel copiar.")
            return
        else:
            raise  # Re-levanta outras excecoes

    # Tenta deletar o arquivo original
    try:
        s3.delete_object(Bucket=bucket_name, Key=del_key)
        print(f"Arquivo {del_key} deletado com sucesso.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"Arquivo {del_key} nao encontrado. Nao foi possivel deletar.")
        else:
            print(f"Erro ao deletar o arquivo {del_key}: {e}")
            raise  # Re-levanta outras excecoes


# ============================================================
# LEITURA DO S3 (spark.read)
# ============================================================

def ler_arquivo_s3(bucket_name, file_key):
    """Le um objeto do S3 e retorna o conteudo como texto UTF-8.

    Parametros:
        bucket_name (str): nome do bucket.
        file_key (str): key do objeto.

    Retorna:
        str: conteudo do arquivo.

    Levanta:
        botocore.exceptions.ClientError: se o objeto nao existir ou nao houver permissao.
        UnicodeDecodeError: se o arquivo nao estiver em UTF-8 (use ler_query_s3 com outro decode).
    """
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    content = response["Body"].read().decode("utf-8")
    return content


def ler_query_s3(bucket_name, file_key, decode="utf-8"):
    """Le um arquivo de texto do S3 (ex.: uma query .sql) com o encoding informado.

    Parametros:
        bucket_name (str): nome do bucket.
        file_key (str): key do arquivo.
        decode (str, opcional): encoding do arquivo. Padrao: "utf-8".

    Retorna:
        str: conteudo do arquivo.

    Exemplo:
        >>> query = ler_query_s3("meu-bucket", "queries/vendas.sql", decode="latin-1")
    """
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    content = response["Body"].read().decode(decode)
    return content


def read_txt_to_df(bucket_name, destination_key, table):
    """Le um TXT separado por TAB (com cabecalho) do S3 como DataFrame Spark, com todas as colunas string.

    Parametros:
        bucket_name (str): nome do bucket.
        destination_key (str): key do arquivo, ou de uma "pasta" (le todos os arquivos dela).
        table (str | None): se informado, registra o DataFrame como view temporaria com esse nome.

    Retorna:
        pyspark.sql.DataFrame
    """
    s3_path = f"s3://{bucket_name}/{destination_key}"
    df = obter_spark().read.option("delimiter", "\t").option("header", "true").csv(s3_path)
    if table:
        df.createOrReplaceTempView(table)
    return df


def read_csv_to_df(bucket_name, destination_key, table):
    """Le um CSV separado por virgula (com cabecalho) do S3 como DataFrame Spark, com todas as colunas string.

    Parametros:
        bucket_name (str): nome do bucket.
        destination_key (str): key do arquivo, ou de uma "pasta" (le todos os arquivos dela).
        table (str | None): se informado, registra o DataFrame como view temporaria com esse nome.

    Retorna:
        pyspark.sql.DataFrame
    """
    s3_path = f"s3://{bucket_name}/{destination_key}"
    df = obter_spark().read.option("delimiter", ",").option("header", "true").csv(s3_path)
    if table:
        df.createOrReplaceTempView(table)
    return df


def read_arquive_to_df(bucket_name, destination_key, table, separator):
    """Le um arquivo delimitado (com cabecalho) do S3 como DataFrame Spark, usando o separador informado.

    Parametros:
        bucket_name (str): nome do bucket.
        destination_key (str): key do arquivo, ou de uma "pasta" (le todos os arquivos dela).
        table (str | None): se informado, registra o DataFrame como view temporaria com esse nome.
        separator (str): separador de colunas (ex.: ";", "|", "\\t").

    Retorna:
        pyspark.sql.DataFrame

    Exemplo:
        >>> df = read_arquive_to_df("meu-bucket", "entrada/clientes.csv", "clientes", ";")
        >>> spark.sql("SELECT COUNT(*) FROM clientes").show()
    """
    s3_path = f"s3://{bucket_name}/{destination_key}"
    df = obter_spark().read.option("delimiter", separator).option("header", "true").csv(s3_path)
    if table:
        df.createOrReplaceTempView(table)
    return df


# ============================================================
# LIMPEZA NO S3
# ============================================================

def apagar_prefixo_s3_client(s3_client, bucket_name, prefixo):
    """Apaga TODOS os objetos cujo caminho comeca com "prefixo", usando um cliente S3 ja criado.

    A exclusao e feita em lotes de ate 1000 objetos (uma pagina do list_objects_v2 por vez).

    Atencao: o prefixo e comparado como texto. "dados/2026" tambem apaga
    "dados/2026_backup/...". Termine o prefixo com "/" para limitar a uma "pasta".

    Parametros:
        s3_client (botocore.client.S3): cliente boto3 de S3.
        bucket_name (str): nome do bucket.
        prefixo (str): prefixo a apagar (nao pode ser vazio).

    Retorna:
        None

    Levanta:
        ValueError: se o prefixo for vazio ou apenas "/" (evita apagar o bucket inteiro).
        RuntimeError: se o S3 reportar falha ao apagar algum objeto.

    Exemplo:
        >>> apagar_prefixo_s3_client(boto3.client("s3"), "meu-bucket", "tmp/vendas/")
    """
    if not prefixo or not str(prefixo).strip("/"):
        # protecao: prefixo vazio apagaria o bucket inteiro
        raise ValueError("Prefixo vazio: operacao cancelada para nao apagar o bucket inteiro.")
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefixo):
        if "Contents" in page:
            objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
            if objects:
                resposta = s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
                erros = resposta.get("Errors") or []
                if erros:
                    raise RuntimeError(f"Falha ao apagar {len(erros)} objeto(s) em s3://{bucket_name}/{prefixo}: {erros[:5]}")


def apaga_prefixo_s3(bucket_name, prefixo):
    """Atalho para apagar_prefixo_s3_client criando o cliente S3 com a configuracao padrao.

    Parametros:
        bucket_name (str): nome do bucket.
        prefixo (str): prefixo a apagar (nao pode ser vazio; termine com "/" para limitar a uma "pasta").

    Retorna:
        None

    Levanta:
        ValueError, RuntimeError: ver apagar_prefixo_s3_client.
    """
    apagar_prefixo_s3_client(boto3.client("s3"), bucket_name, prefixo)
