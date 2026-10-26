"""athena_export - Converte arquivos TXT do S3 em Parquet e cria/atualiza a tabela no Athena / Glue Data Catalog (via Spark).

Funcao recomendada: exporta_parquet_particionado_athena, que cria a tabela
externa por DDL, regrava apenas as particoes processadas e as registra com
ALTER TABLE ADD PARTITION (sem saveAsTable).

Convencao de nomes dos TXT: "<prefixo_txt>/<nome>_<YYYYMMDD>.txt", separados
por TAB e com cabecalho. Todas as colunas sao tratadas como string.
Nenhum bucket ou database e fixo: todos sao parametros.
"""

import boto3
from pyspark.sql.functions import col

from utils_gerais import obter_spark
from funcoes_s3 import apaga_prefixo_s3
from schema_normalize import (
    create_schema_from_s3_txt,
    read_txt_to_df_with_schema,
    create_schema_from_s3_txt_normalizado,
    read_txt_to_df_with_schema_normalizado,
)
from funcoes_athena import schema_athena_txt, columns_definition_athena


def create_athena_table(df_execucao_atual, destination_key_s3_folder_hist, destination_key_s3_folder_parquets,
                        tabela_destino, database_destino, bucket_name):
    """Converte o historico em TXT de uma pasta do S3 para Parquet (sem particoes) e cria a tabela no Glue Data Catalog.

    Passos:
        1. le todos os TXT de s3://<bucket_name>/<destination_key_s3_folder_hist>/
           (TAB, cabecalho, charset cp1252), usando as colunas de df_execucao_atual
           como schema (todas string);
        2. grava tudo em Parquet (modo overwrite) em
           s3://<bucket_name>/<destination_key_s3_folder_parquets>/;
        3. cria a tabela <database_destino>.<tabela_destino> no Glue apontando para essa pasta.
           Se a tabela ja existir, apenas avisa (os dados Parquet ja foram atualizados).

    Parametros:
        df_execucao_atual (pyspark.sql.DataFrame): DataFrame com as mesmas colunas,
            na mesma ordem, dos TXT (usado apenas para montar o schema).
        destination_key_s3_folder_hist (str): pasta dos TXT historicos (sem "/" no final).
        destination_key_s3_folder_parquets (str): pasta de destino do Parquet
            (todo o conteudo e sobrescrito).
        tabela_destino (str): nome da tabela a criar.
        database_destino (str): database do Glue Data Catalog.
        bucket_name (str): bucket de origem e destino.

    Retorna:
        None

    Levanta:
        Exception: qualquer erro de leitura, gravacao ou criacao da tabela (o erro
            tambem e impresso).
    """
    spark = obter_spark()
    try:
        # Le o empilhamento do historico
        df_athena = spark \
            .read \
            .option('delimiter', '\t') \
            .option('charset', 'cp1252') \
            .option('header', True) \
            .option('inferSchema', False) \
            .schema(schema_athena_txt(df_execucao_atual)) \
            .format('csv') \
            .load(f's3://{bucket_name}/{destination_key_s3_folder_hist}/')

        # Gravar historico como parquet no S3
        df_athena \
            .write \
            .mode("overwrite") \
            .format('parquet') \
            .save(
                f"s3://{bucket_name}/{destination_key_s3_folder_parquets}/"
            )

        # Definir a estrutura da tabela de forma dinamica
        definicao_tabela = {
            "Name": f'{tabela_destino}',
            "StorageDescriptor": {
                "Columns": columns_definition_athena(df_execucao_atual),
                "Location": f"s3://{bucket_name}/{destination_key_s3_folder_parquets}/",
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                }
            }
        }

        glueClient = boto3.client('glue')
        try:
            glueClient.create_table(
                DatabaseName=database_destino,
                TableInput=definicao_tabela
            )
        except glueClient.exceptions.AlreadyExistsException:
            print(f"Tabela {database_destino}.{tabela_destino} ja existe no catalogo. Dados Parquet atualizados.")
    except Exception as e:
        print(f"Erro ao criar a tabela {database_destino}.{tabela_destino}: {e}")
        raise


def exporta_parquet_cria_athena(partition_column, bucket_name, prefixo_txt, prefixo_parquet, database_destino, tabela_destino, data_d1, reprocessar_tudo="N"):
    """Exporta o TXT do dia (ou todo o historico) para Parquet particionado e registra a tabela <database_destino>.<tabela_destino> via saveAsTable.

    O schema e lido do cabecalho do TXT do dia (todas as colunas string).
    Equivale a exporta_parquet_cria_athena_schema com schema=None.

    Atencao: a gravacao usa saveAsTable em modo "overwrite". Dependendo da
    versao do Spark/Glue, isso recria a tabela e pode sobrescrever todo o
    conteudo de prefixo_parquet, e nao apenas a particao do dia. Para atualizar
    somente as particoes processadas, use exporta_parquet_particionado_athena.

    Parametros:
        partition_column (str): coluna do TXT usada como particao (ex.: "data_ref").
        bucket_name (str): bucket onde estao os TXT e onde o Parquet sera gravado.
        prefixo_txt (str): "pasta" dos arquivos TXT (sem "/" no final).
        prefixo_parquet (str): "pasta" de destino do Parquet (LOCATION da tabela).
        database_destino (str): database do Glue Data Catalog.
        tabela_destino (str): nome da tabela. Tambem e o nome base dos TXT: o arquivo do
            dia e "<prefixo_txt>/<tabela_destino>_<data_d1>.txt".
        data_d1 (str): data do arquivo do dia, "YYYYMMDD".
        reprocessar_tudo (str, opcional): "N" processa so o TXT do dia; "S" le TODOS
            os arquivos de prefixo_txt (reprocessa o historico). Padrao: "N".

    Retorna:
        None

    Levanta:
        ValueError: se reprocessar_tudo nao for "S" nem "N".
    """
    return exporta_parquet_cria_athena_schema(
        partition_column=partition_column, bucket_name=bucket_name, prefixo_txt=prefixo_txt,
        prefixo_parquet=prefixo_parquet, database_destino=database_destino, tabela_destino=tabela_destino, data_d1=data_d1,
        schema=None, reprocessar_tudo=reprocessar_tudo,
    )


def exporta_parquet_cria_athena_schema(partition_column, bucket_name, prefixo_txt, prefixo_parquet, database_destino, tabela_destino, data_d1, schema=None, reprocessar_tudo="N"):
    """Exporta o TXT do dia (ou todo o historico) para Parquet particionado e registra a tabela via saveAsTable, com schema opcional.

    Atencao: a gravacao usa saveAsTable em modo "overwrite". Dependendo da
    versao do Spark/Glue, isso recria a tabela e pode sobrescrever todo o
    conteudo de prefixo_parquet, e nao apenas a particao do dia. Para atualizar
    somente as particoes processadas, use exporta_parquet_particionado_athena.

    Parametros:
        partition_column (str): coluna do TXT usada como particao (ex.: "data_ref").
        bucket_name (str): bucket onde estao os TXT e onde o Parquet sera gravado.
        prefixo_txt (str): "pasta" dos arquivos TXT (sem "/" no final).
        prefixo_parquet (str): "pasta" de destino do Parquet (LOCATION da tabela).
        database_destino (str): database do Glue Data Catalog.
        tabela_destino (str): nome da tabela. Tambem e o nome base dos TXT: o arquivo do
            dia e "<prefixo_txt>/<tabela_destino>_<data_d1>.txt".
        data_d1 (str): data do arquivo do dia, "YYYYMMDD".
        schema (pyspark.sql.types.StructType, opcional): schema de leitura dos TXT,
            na ordem das colunas do arquivo. Se None, e gerado a partir do
            cabecalho do TXT do dia (todas as colunas string).
        reprocessar_tudo (str, opcional): "N" processa so o TXT do dia; "S" le TODOS
            os arquivos de prefixo_txt (reprocessa o historico). Padrao: "N".

    Retorna:
        None

    Levanta:
        ValueError: se reprocessar_tudo nao for "S" nem "N".
    """
    spark = obter_spark()
    reprocessar_tudo = str(reprocessar_tudo).strip().upper()
    if reprocessar_tudo not in ("S", "N"):
        raise ValueError("O parametro reprocessar_tudo deve ser 'S' ou 'N'.")

    if reprocessar_tudo == "S":
        s3_path = f"s3://{bucket_name}/{prefixo_txt}/{tabela_destino}_{data_d1}.txt"
        if schema is None:
            schema = create_schema_from_s3_txt(s3_path)
        df_athena = spark \
            .read \
            .option('delimiter', '\t') \
            .option('encoding', 'UTF-8') \
            .option('header', True) \
            .option('inferSchema', False) \
            .schema(schema) \
            .format('csv') \
            .load(f's3://{bucket_name}/{prefixo_txt}/')
    else:
        s3_path = f"s3://{bucket_name}/{prefixo_txt}/{tabela_destino}_{data_d1}.txt"
        if schema is None:
            schema = create_schema_from_s3_txt(s3_path)
        df_dia_atual = read_txt_to_df_with_schema(s3_path, schema)
        df_athena = df_dia_atual

    df_athena \
        .write \
        .mode("overwrite") \
        .option("partitionOverwriteMode", "dynamic") \
        .format("parquet") \
        .option("path", f"s3://{bucket_name}/{prefixo_parquet}") \
        .partitionBy(f"{partition_column}") \
        .saveAsTable(f"{database_destino}.{tabela_destino}")


def gera_ddl_create_external_parquet(
    database_destino,
    nome_tabela,
    schema,
    partition_column,
    bucket_name,
    prefixo_parquet
):
    """Gera o DDL "CREATE EXTERNAL TABLE IF NOT EXISTS" de uma tabela Parquet particionada no Athena/Glue.

    Todas as colunas (inclusive a de particao) sao declaradas como string, e os
    nomes vao entre crases, o que permite caracteres especiais.

    Parametros:
        database_destino (str): database.
        nome_tabela (str): nome da tabela.
        schema (pyspark.sql.types.StructType): schema com todas as colunas, incluindo a de particao.
        partition_column (str): coluna de particao (sai da lista de colunas e vai para PARTITIONED BY).
        bucket_name (str): bucket do LOCATION.
        prefixo_parquet (str): prefixo do LOCATION (sem "/" nas pontas).

    Retorna:
        str: comando DDL pronto para spark.sql ou Athena.

    Levanta:
        ValueError: se partition_column nao estiver no schema ou se ela for a unica coluna.

    Exemplo:
        >>> print(gera_ddl_create_external_parquet("meu_database", "vendas", schema,
        ...                                        "data_ref", "meu-bucket", "parquet/vendas"))
    """
    colunas_schema = [field.name for field in schema.fields]

    if partition_column not in colunas_schema:
        raise ValueError(
            f"A coluna de particao '{partition_column}' nao existe no schema. "
            f"Colunas encontradas: {colunas_schema}"
        )

    colunas_comuns = [
        field.name for field in schema.fields
        if field.name != partition_column
    ]

    if len(colunas_comuns) == 0:
        raise ValueError(
            "O schema ficou sem colunas comuns. "
            "Apenas a coluna de particao foi encontrada."
        )

    ddl_colunas = ",\n  ".join([
        f"  `{nome_coluna}` string"
        for nome_coluna in colunas_comuns
    ])

    location_tabela = f"s3://{bucket_name}/{prefixo_parquet}/"

    ddl = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS `{database_destino}`.`{nome_tabela}` (
{ddl_colunas}
)
PARTITIONED BY (
  `{partition_column}` string
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '{location_tabela}'
TBLPROPERTIES (
  'classification'='parquet',
  'parquet.compression'='SNAPPY'
)"""

    return ddl.strip()


def exporta_parquet_particionado_athena(
    partition_column,
    bucket_name,
    prefixo_txt,
    prefixo_parquet,
    database_destino,
    tabela_destino,
    data_d1,
    reprocessar_tudo="N",
    delimiter="\t",
    usar_msck_no_historico=False,
    nome_txt_base=None
):
    """Converte TXT do S3 em Parquet particionado e cria/atualiza a tabela no Athena, regravando apenas as particoes processadas.

    Passos:
        1. le o cabecalho do TXT do dia e monta o schema (todas as colunas string),
           validando colunas vazias/duplicadas;
        2. executa CREATE EXTERNAL TABLE IF NOT EXISTS (tabela existente nao e alterada);
        3. le o TXT do dia (reprocessar_tudo="N") ou todos os TXT de prefixo_txt ("S");
        4. descarta linhas com a coluna de particao nula e valida o resultado;
        5. apaga no S3 somente as particoes presentes nos dados e grava o Parquet (append);
        6. registra as particoes com ALTER TABLE ADD IF NOT EXISTS PARTITION (ou
           MSCK REPAIR TABLE, com reprocessar_tudo="S" e usar_msck_no_historico=True).

    Parametros:
        partition_column (str): coluna do TXT usada como particao (ex.: "data_ref").
        bucket_name (str): bucket onde estao os TXT e onde o Parquet sera gravado.
        prefixo_txt (str): "pasta" dos arquivos TXT (sem "/" no final).
        prefixo_parquet (str): "pasta" de destino do Parquet (LOCATION da tabela).
        database_destino (str): database do Glue Data Catalog.
        tabela_destino (str): nome da tabela no Glue Data Catalog.
        data_d1 (str): data do arquivo do dia, "YYYYMMDD".
        reprocessar_tudo (str, opcional): "N" processa so o TXT do dia; "S" le TODOS
            os arquivos de prefixo_txt (reprocessa o historico). Padrao: "N".
        delimiter (str, opcional): separador de colunas dos TXT. Padrao: "\\t".
        usar_msck_no_historico (bool, opcional): com reprocessar_tudo="S", usa
            MSCK REPAIR TABLE para registrar as particoes. Padrao: False.
        nome_txt_base (str, opcional): nome base dos TXT, se diferente do nome da
            tabela; o arquivo do dia e "<prefixo_txt>/<nome_txt_base>_<data_d1>.txt".
            Padrao: o valor de tabela_destino.

    Retorna:
        None. Imprime o DDL, as quantidades de linhas e as particoes atualizadas.

    Levanta:
        ValueError: reprocessar_tudo invalido; coluna de particao ausente; nenhum
            dado (ou todas as particoes nulas); ou, com reprocessar_tudo="N", mais de
            uma particao nos dados do dia; cabecalho invalido.

    Exemplo:
        >>> exporta_parquet_particionado_athena(
        ...     partition_column="data_ref", bucket_name="meu-bucket",
        ...     prefixo_txt="txt/vendas", prefixo_parquet="parquet/vendas",
        ...     database_destino="meu_database", tabela_destino="vendas", data_d1="20260927")
    """
    if reprocessar_tudo not in ("S", "N"):
        raise ValueError("O parametro reprocessar_tudo deve ser 'S' ou 'N'.")

    spark = obter_spark()

    prefixo_txt_limpo = prefixo_txt.strip("/")
    prefixo_parquet_limpo = prefixo_parquet.strip("/")

    # compatibilidade retroativa
    if nome_txt_base is None:
        nome_txt_base = tabela_destino

    s3_path_dia = f"s3://{bucket_name}/{prefixo_txt_limpo}/{nome_txt_base}_{data_d1}.txt"
    path_parquet = f"s3://{bucket_name}/{prefixo_parquet_limpo}/"

    # 1. Cria schema a partir do TXT de referencia
    schema = create_schema_from_s3_txt_normalizado(
        s3_path=s3_path_dia,
        delimiter=delimiter
    )

    # 2. Cria tabela Athena/Glue se nao existir
    ddl_create = gera_ddl_create_external_parquet(
        database_destino=database_destino,
        nome_tabela=tabela_destino,
        schema=schema,
        partition_column=partition_column,
        bucket_name=bucket_name,
        prefixo_parquet=prefixo_parquet_limpo
    )

    print("DDL que sera executado para garantir existencia da tabela:")
    print(ddl_create)

    spark.sql(ddl_create)

    # 3. Le dados
    if reprocessar_tudo == "S":
        path_leitura = f"s3://{bucket_name}/{prefixo_txt_limpo}/"

        df_athena = (
            spark
            .read
            .option("delimiter", delimiter)
            .option("encoding", "UTF-8")
            .option("header", True)
            .option("inferSchema", False)
            .schema(schema)
            .format("csv")
            .load(path_leitura)
        )
    else:
        df_athena = read_txt_to_df_with_schema_normalizado(
            s3_path=s3_path_dia,
            schema=schema,
            delimiter=delimiter
        )

    # 4. Validacoes antes de gravar
    if partition_column not in df_athena.columns:
        raise ValueError(
            f"Coluna de particao '{partition_column}' nao existe no DataFrame. "
            f"Colunas disponiveis: {df_athena.columns}"
        )

    qtd_linhas_original = df_athena.count()

    if qtd_linhas_original == 0:
        raise ValueError(
            "O DataFrame esta vazio. Nenhum dado sera gravado. "
            f"Tabela: {database_destino}.{tabela_destino}"
        )

    df_athena = df_athena.filter(col(partition_column).isNotNull())

    qtd_linhas_validas = df_athena.count()

    if qtd_linhas_validas == 0:
        raise ValueError(
            f"Todas as linhas estao com '{partition_column}' nulo. "
            "Nenhuma particao sera gravada."
        )

    particoes = [
        str(row[partition_column])
        for row in df_athena.select(partition_column).distinct().collect()
    ]

    if len(particoes) == 0:
        raise ValueError("Nenhuma particao valida foi identificada.")

    if reprocessar_tudo == "N" and len(particoes) > 1:
        raise ValueError(
            f"Encontradas {len(particoes)} particoes: {particoes}. "
            "Para processamento diario (reprocessar_tudo='N'), apenas uma particao e esperada."
        )

    print(f"Quantidade de linhas original: {qtd_linhas_original}")
    print(f"Quantidade de linhas validas: {qtd_linhas_validas}")
    print(f"Particoes que serao atualizadas: {particoes}")

    # 5. Limpa particoes antes de gravar
    for valor_particao in particoes:
        prefixo_particao = (
            f"{prefixo_parquet_limpo}/"
            f"{partition_column}={valor_particao}/"
        )

        print(f"Limpando particao: s3://{bucket_name}/{prefixo_particao}")

        apaga_prefixo_s3(
            bucket_name=bucket_name,
            prefixo=prefixo_particao
        )

    # 6. Grava Parquet no S3 sem recriar tabela
    df_athena \
        .write \
        .mode("append") \
        .format("parquet") \
        .partitionBy(partition_column) \
        .save(path_parquet)

    print(f"Dados gravados em: {path_parquet}")

    # 7. Atualiza particoes no catalogo
    if reprocessar_tudo == "S" and usar_msck_no_historico:
        comando_msck = f"MSCK REPAIR TABLE `{database_destino}`.`{tabela_destino}`"

        print(f"Executando: {comando_msck}")

        spark.sql(comando_msck)
    else:
        for valor_particao in particoes:
            location_particao = (
                f"s3://{bucket_name}/{prefixo_parquet_limpo}/"
                f"{partition_column}={valor_particao}/"
            )

            comando_alter = (
                f"ALTER TABLE `{database_destino}`.`{tabela_destino}` "
                f"ADD IF NOT EXISTS PARTITION "
                f"(`{partition_column}`='{valor_particao}') "
                f"LOCATION '{location_particao}'"
            )

            print(f"Executando: {comando_alter}")

            spark.sql(comando_alter)

    # 8. Validacao final basica
    print(f"Processo concluido com sucesso para {database_destino}.{tabela_destino}.")
    print(f"Particoes atualizadas: {particoes}")
