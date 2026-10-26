"""funcoes_athena - Leitura e escrita com Amazon Athena / Glue Data Catalog.

- Leitura: tabela do Glue Catalog como view Spark (read_table), predicado da
  particao mais recente (filtro_particao_mais_recente), query Athena para pandas/Spark
  (run_athena_query, athena_to_pandas, query_athena_df_spark*).
- Escrita: ParquetAthenaExporter / criar_tabela_athena_parquet, que gravam um
  DataFrame pandas em Parquet no S3 e criam/atualizam a tabela externa no Athena.

Nenhum bucket, database ou workgroup e fixo: todos sao parametros.
As funcoes de exportacao a partir de TXT no S3 (via Spark) estao em athena_export.
"""

import os
import time
import shutil
from typing import List, Optional
from dataclasses import dataclass

import boto3
import pandas as pd

try:
    import awswrangler as wr
except ImportError:  # permite usar ParquetAthenaExporter/run_athena_query fora do Glue
    wr = None

from utils_gerais import obter_spark, obter_glue_context, obter_logger, normalizar_valor_particao
from funcoes_s3 import apagar_prefixo_s3_client

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.dataset as ds
except ImportError:
    pa = None
    pq = None
    ds = None


# ============================================================
# LEITURA
# ============================================================

def read_table(database, table, filter='1=1', columns=None, additional_options=None, **kwargs):
    """Le uma tabela do Glue Data Catalog como DataFrame Spark e a registra como view temporaria.

    A view recebe o nome "<database>_<table>" e pode ser usada em spark.sql.
    Os modos de rebase de datas (int96/datetime) sao fixados em "CORRECTED" para
    evitar erros com datas antigas em Parquet.

    Parametros:
        database (str): database no Glue Data Catalog.
        table (str): tabela no Glue Data Catalog.
        filter (str, opcional): predicado aplicado as COLUNAS DE PARTICAO
            (push_down_predicate), escrito como um WHERE do SQL, ex.:
            "data_particao >= '20260901'". Padrao: "1=1" (todas as particoes).
        columns (list[str], opcional): colunas a manter. Padrao: todas.
        additional_options (dict, opcional): opcoes extras do Glue, ex.:
            {"catalogPartitionPredicate": "data_particao >= '20260901'"}.
        **kwargs: mais opcoes, somadas a additional_options.

    Retorna:
        pyspark.sql.DataFrame

    Levanta:
        RuntimeError: se o filtro nao retornar nenhum registro (a contagem e feita
            com um COUNT(*), que le os dados filtrados).

    Requer:
        ambiente AWS Glue (awsglue).

    Exemplo:
        >>> df = read_table("meu_database", "minha_tabela",
        ...                 filter="data_particao >= '20260901'", columns=["id", "valor"])
        >>> spark.sql("SELECT COUNT(*) FROM meu_database_minha_tabela").show()
    """
    spark = obter_spark()
    glueContext = obter_glue_context()
    logger = obter_logger()

    view = f"{database}_{table}"

    # copia para nao alterar o dicionario de quem chamou
    additional_options = dict(additional_options or {})
    additional_options.update(kwargs)

    additional_options.update({
        "int96RebaseModeInWrite": "CORRECTED",
        "int96RebaseModeInRead": "CORRECTED",
        "datetimeRebaseModeInWrite": "CORRECTED",
        "datetimeRebaseModeInRead": "CORRECTED"
    })

    df = glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table,
        push_down_predicate=filter,
        additional_options=additional_options
    ).toDF()

    if columns:
        df = df.select(*columns)

    df.createOrReplaceTempView(view)

    count_df = spark.sql(f"SELECT COUNT(*) AS total FROM {view}").collect()[0]["total"]

    if count_df > 0:
        logger.info(f"origem {table} registrada com sucesso. Encontrados {count_df} registros para o filtro '{filter}'.")
    else:
        msg = f"A origem '{table}' apresentou {count_df} registros para o filtro {filter}. Execucao cancelada."
        logger.error(msg)
        raise RuntimeError(msg)

    return df


def filtro_particao_mais_recente(database, tabela, ignora_coluna=None):
    """Monta um predicado SQL que seleciona o MAIOR valor de cada coluna de particao da tabela.

    Le "SHOW PARTITIONS" e, para cada coluna de particao, pega o maior valor
    (comparacao de texto). O resultado pode ser usado como filtro de read_table.

    Atencao: o maximo e calculado coluna a coluna, de forma independente. Em
    tabelas com mais de uma coluna de particao, a combinacao resultante pode nao
    existir (ex.: maior "ano" + maior "mes" de outro ano). Use ignora_coluna para
    desconsiderar colunas como "nome_arquivo". Por ser comparacao de texto, datas
    precisam estar em formato ordenavel (YYYYMMDD / YYYY-MM-DD).

    Parametros:
        database (str): database da tabela.
        tabela (str): nome da tabela.
        ignora_coluna (str | list[str], opcional): coluna(s) de particao a desconsiderar.

    Retorna:
        str: ex.: "CAST(data_particao AS string) = '20260927'" (colunas unidas por AND).

    Levanta:
        RuntimeError: se a tabela nao tiver particoes.
        KeyError: se uma coluna de ignora_coluna nao existir.

    Exemplo:
        >>> filtro = filtro_particao_mais_recente("meu_database", "minha_tabela")
        >>> df = read_table("meu_database", "minha_tabela", filter=filtro)
    """
    spark = obter_spark()
    logger = obter_logger()

    df_particoes = spark.sql(f"SHOW PARTITIONS {database}.{tabela}").toPandas()

    if df_particoes.empty:
        raise RuntimeError(f"A tabela {database}.{tabela} nao possui particoes.")

    valores_particao = df_particoes['partition'].str.split("/", expand=True)

    nomes_colunas = {}

    for c in valores_particao.columns:
        nomes_colunas[c] = (
            valores_particao.iloc[0, c]
            .split("=")[0]
            .lower()
        )
        valores_particao[c] = valores_particao[c].str.split("=", n=1).str[1]

    if ignora_coluna:
        ignora_coluna = [c.lower() for c in ([ignora_coluna] if isinstance(ignora_coluna, str) else ignora_coluna)]
        valores_particao.rename(nomes_colunas, axis=1, inplace=True)
        valores_particao = valores_particao.drop(columns=ignora_coluna)
        valores_particao = valores_particao.drop_duplicates()
    else:
        valores_particao.rename(nomes_colunas, axis=1, inplace=True)
        valores_particao = valores_particao.drop_duplicates()

    predicado = valores_particao.apply(lambda x: x.max(), axis=0)

    predicado = " AND ".join([f"CAST({coluna} AS string) = '{valor}'" for coluna, valor in predicado.items()])

    logger.info(f"Filtro da particao mais recente: {predicado}")

    return predicado


def query_athena_df_spark(query, table, database, workgroup):
    """Executa uma query no Athena e devolve o resultado como DataFrame Spark com todas as colunas string.

    O resultado e baixado para pandas (awswrangler), convertido com
    schema_normalize.normalize_df e registrado como view temporaria "table".
    Todo o resultado passa pela memoria do driver: use para volumes moderados.

    Parametros:
        query (str): SQL a executar no Athena.
        table (str): nome da view temporaria criada no Spark.
        database (str): database do Athena/Glue Catalog onde a query roda.
        workgroup (str): workgroup do Athena (precisa ter local de resultados configurado).

    Retorna:
        pyspark.sql.DataFrame

    Requer:
        awswrangler e pyspark.

    Exemplo:
        >>> df = query_athena_df_spark("SELECT * FROM clientes LIMIT 10", "clientes_tmp",
        ...                            database="meu_database", workgroup="meu-workgroup")
        >>> spark.sql("SELECT COUNT(*) FROM clientes_tmp").show()
    """
    import gc
    from schema_normalize import normalize_df

    _exigir_awswrangler()

    # Carregar os dados da query em um DataFrame
    df = wr.athena.read_sql_query(
        sql=query,
        database=database,
        ctas_approach=False,
        workgroup=workgroup
    )

    # Normalizar os dados (pandas) e converter para PySpark
    df_spark = normalize_df(df)

    # Liberacao de memoria
    del df
    gc.collect()  # Forca a coleta de lixo

    df_spark.createOrReplaceTempView(table)

    return df_spark


def query_athena_df_spark_auto_schema(query, table, database, workgroup):
    """Igual a query_athena_df_spark, mas mantendo os tipos das colunas (inferidos pelo pandas).

    Usa schema_normalize.pandas_to_spark_auto_schema + normalize_df_schema.

    Parametros:
        query (str): SQL a executar no Athena.
        table (str): nome da view temporaria criada no Spark.
        database (str): database do Athena/Glue Catalog onde a query roda.
        workgroup (str): workgroup do Athena (precisa ter local de resultados configurado).

    Retorna:
        pyspark.sql.DataFrame

    Requer:
        awswrangler e pyspark.
    """
    import gc
    from schema_normalize import pandas_to_spark_auto_schema, normalize_df_schema

    _exigir_awswrangler()

    # Carregar os dados da query em um DataFrame
    df = wr.athena.read_sql_query(
        sql=query,
        database=database,
        ctas_approach=False,
        workgroup=workgroup
    )

    # Gerar automaticamente o esquema PySpark com base nos tipos do DataFrame Pandas
    schema = pandas_to_spark_auto_schema(df)

    # Normalizar os dados e aplicar o esquema gerado
    df_spark = normalize_df_schema(df, schema)

    # Liberacao de memoria
    del df
    gc.collect()  # Forca a coleta de lixo

    df_spark.createOrReplaceTempView(table)

    return df_spark


def athena_to_pandas(query, database, workgroup):
    """Executa uma query no Athena (via awswrangler) e retorna um DataFrame pandas, sem interromper o fluxo em caso de erro.

    Parametros:
        query (str): SQL a executar.
        database (str): database do Athena/Glue Catalog onde a query roda.
        workgroup (str): workgroup do Athena (precisa ter local de resultados configurado).

    Retorna:
        pandas.DataFrame: resultado da query, ou DataFrame VAZIO se ocorrer
        qualquer erro (o erro e apenas impresso). Confira se o resultado esta
        vazio antes de seguir.

    Requer:
        awswrangler.
    """
    try:
        _exigir_awswrangler()
        df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            ctas_approach=False,
            workgroup=workgroup
        )

        print("Consulta capturada do Athena com sucesso!")
        return df

    except Exception as e:
        print(f"Erro ao executar a consulta no Athena: {e}")
        return pd.DataFrame()


# ============================================================
# SCHEMA ATHENA
# ============================================================

def schema_athena_txt(table_atual):
    """Cria um StructType com todas as colunas de um DataFrame Spark como StringType.

    Parametros:
        table_atual (pyspark.sql.DataFrame): DataFrame de referencia (usa apenas .columns).

    Retorna:
        pyspark.sql.types.StructType
    """
    from pyspark.sql.types import StructType, StructField, StringType

    column_names = table_atual.columns

    schema = StructType([StructField(col_name, StringType(), True) for col_name in column_names])

    return schema


def columns_definition_athena(table_atual):
    """Gera a lista de colunas no formato do Glue (TableInput.StorageDescriptor.Columns), todas como string.

    Parametros:
        table_atual (pyspark.sql.DataFrame): DataFrame de referencia (usa apenas .columns).

    Retorna:
        list[dict]: [{"Name": "COLUNA", "Type": "string"}, ...]; os nomes ficam em MAIUSCULAS.
    """
    # Obter os nomes das colunas do DataFrame Spark
    column_names_athena = table_atual.columns

    # Criar a estrutura de Columns automaticamente
    columns_definition = [{'Name': col_name.upper(), 'Type': 'string'} for col_name in column_names_athena]

    return columns_definition


# ============================================================
# CONSULTA E CONVERSAO ATHENA
# ============================================================

def run_athena_query(query, database, workgroup, output_location=None):
    """Executa uma query no Athena usando apenas boto3 e retorna o resultado como DataFrame pandas.

    Aguarda a conclusao consultando o status a cada segundo e le todas as paginas
    do resultado (sem o limite de 1000 linhas de uma chamada unica).

    Parametros:
        query (str): SQL a executar.
        database (str): database do Athena/Glue Catalog onde a query roda.
        workgroup (str): workgroup do Athena (precisa ter local de resultados configurado).
        output_location (str, opcional): "s3://bucket/prefixo/" para os arquivos de
            resultado. Se None, usa o local configurado no workgroup.

    Retorna:
        pandas.DataFrame: todas as colunas como texto (valores nulos viram None).

    Levanta:
        Exception: se a query terminar como FAILED ou CANCELLED (com o motivo).

    Exemplo:
        >>> df = run_athena_query("SELECT COUNT(*) AS qtd FROM clientes",
        ...                       database="meu_database", workgroup="meu-workgroup")
    """
    athena = boto3.client("athena")
    parametros = {
        "QueryString": query,
        "QueryExecutionContext": {"Database": database},
        "WorkGroup": workgroup,
    }
    if output_location:
        parametros["ResultConfiguration"] = {"OutputLocation": output_location}
    response = athena.start_query_execution(**parametros)
    query_execution_id = response["QueryExecutionId"]

    while True:
        status_response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = status_response["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            break
        elif state in ["FAILED", "CANCELLED"]:
            reason = status_response["QueryExecution"]["Status"].get("StateChangeReason", "No reason provided")
            raise Exception(f"Query falhou com status: {state}. Motivo: {reason}")

        time.sleep(1)

    # get_query_results devolve no maximo 1000 linhas por chamada: paginar.
    paginator = athena.get_paginator("get_query_results")
    columns = None
    data = []
    primeira_pagina = True
    for page in paginator.paginate(QueryExecutionId=query_execution_id):
        if columns is None:
            columns = [col["Label"] for col in page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
        rows = page["ResultSet"]["Rows"]
        if primeira_pagina:
            rows = rows[1:]  # a primeira linha da primeira pagina e o cabecalho
            primeira_pagina = False
        for row in rows:
            data.append([field.get("VarCharValue") for field in row["Data"]])

    return pd.DataFrame(data, columns=columns or [])


def query_athena(*args, **kwargs):
    """Igual a run_athena_query (mesmos parametros), mas retorna um DataFrame VAZIO em caso de erro, em vez de levantar excecao.

    O erro e apenas impresso. Util em rotinas exploratorias; em cargas, prefira
    run_athena_query para nao seguir com dados vazios sem perceber.

    Retorna:
        pandas.DataFrame
    """
    try:
        return run_athena_query(*args, **kwargs)
    except Exception as e:
        print(f"Erro ao executar query: {e}")
        return pd.DataFrame()


def pa_to_athena(pa_type) -> str:
    """Converte um tipo PyArrow no tipo equivalente do Athena.

    int8/16/32 -> integer, int64 -> bigint, float32 -> real, float64 -> double,
    bool -> boolean, string -> string, timestamp -> timestamp, date -> date,
    decimal(p, s) -> decimal(p, s), list<T> -> array<T>. Demais tipos -> string.

    Parametros:
        pa_type (pyarrow.DataType): tipo de um campo do schema PyArrow.

    Retorna:
        str: tipo do Athena.
    """
    if pa is None:
        return "string"
    if pa.types.is_int8(pa_type) or pa.types.is_int16(pa_type) or pa.types.is_int32(pa_type):
        return "integer"
    if pa.types.is_int64(pa_type):
        return "bigint"
    if pa.types.is_float32(pa_type):
        return "real"
    if pa.types.is_float64(pa_type):
        return "double"
    if pa.types.is_boolean(pa_type):
        return "boolean"
    if pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
        return "string"
    if pa.types.is_timestamp(pa_type):
        return "timestamp"
    if pa.types.is_date(pa_type):
        return "date"
    if pa.types.is_decimal(pa_type):
        return f"decimal({pa_type.precision}, {pa_type.scale})"
    if pa.types.is_list(pa_type):
        value_type = pa_to_athena(pa_type.value_type)
        return f"array<{value_type}>"
    return "string"


def _exigir_awswrangler():
    """Levanta ImportError com mensagem clara se o awswrangler nao estiver instalado."""
    if wr is None:
        raise ImportError("awswrangler nao esta instalado neste ambiente.")


# ============================================================
# EXPORTACAO PANDAS -> PARQUET -> ATHENA
# ============================================================

@dataclass
class AthenaParquetConfig:
    """Configuracao do ParquetAthenaExporter (exportacao de um DataFrame pandas para uma tabela Athena).

    Atributos obrigatorios:
        df (pandas.DataFrame): dados a exportar (nao pode estar vazio).
        nome_parquet (str | None): nome do arquivo .parquet no S3 (sem extensao)
            quando nao ha particoes. None usa o nome da tabela.
        bucket_name (str): bucket de destino.
        pasta_no_bucket (str): prefixo S3 da tabela (vira o LOCATION).
        database (str): database do Athena/Glue.
        tabela (str): nome da tabela.
        caminho_parquet_local (str): arquivo .parquet local temporario. Com
            particoes, a pasta "<caminho sem extensao>/" e usada (e APAGADA antes).
        profile_aws (str | None): perfil do ~/.aws/config; None usa as credenciais padrao.
        workgroup (str): workgroup do Athena.

    Atributos opcionais:
        drop_if_exists (bool): executa DROP TABLE antes de criar. Padrao: False.
        confirmar_drop (bool): precisa ser True junto com drop_if_exists (protecao). Padrao: False.
        partition_cols (list[str] | None): colunas de particao (formato Hive). Padrao: None.
        limpar_particoes_antes_upload (bool): apaga no S3 as particoes que serao
            regravadas, evitando arquivos duplicados. Padrao: True.
        usar_msck (bool): registra particoes com MSCK REPAIR TABLE em vez de
            ALTER TABLE ADD PARTITION. Padrao: False.
        athena_results_prefix (str): prefixo S3 (no mesmo bucket) para os
            resultados das queries DDL. OBRIGATORIO na pratica: e APAGADO antes de
            cada DDL e nao pode ficar dentro de pasta_no_bucket.
        falhar_se_particao_nula (bool): erro se houver particao nula; se False,
            essas linhas sao descartadas com aviso. Padrao: True.
        forcar_particoes_como_string (bool): converte os valores de particao para
            texto (ver utils_gerais.normalizar_valor_particao) e declara as
            particoes como string no DDL. Padrao: True.
    """
    df: "pd.DataFrame"
    nome_parquet: str
    bucket_name: str
    pasta_no_bucket: str
    database: str
    tabela: str
    caminho_parquet_local: str
    profile_aws: str
    workgroup: str
    drop_if_exists: bool = False
    confirmar_drop: bool = False
    partition_cols: Optional[List[str]] = None
    limpar_particoes_antes_upload: bool = True
    usar_msck: bool = False
    athena_results_prefix: Optional[str] = None
    falhar_se_particao_nula: bool = True
    forcar_particoes_como_string: bool = True


class ParquetAthenaExporter:
    """Exporta um DataFrame pandas para Parquet no S3 e cria/atualiza a tabela externa correspondente no Athena.

    Fluxo de run():
        1. grava o Parquet localmente (particionado no formato Hive, se houver partition_cols);
        2. apaga no S3 as particoes que serao regravadas (limpar_particoes_antes_upload);
        3. envia os arquivos para s3://<bucket_name>/<pasta_no_bucket>/;
        4. executa CREATE EXTERNAL TABLE IF NOT EXISTS (com DROP antes, se configurado);
        5. registra as particoes (ALTER TABLE ADD IF NOT EXISTS PARTITION ou MSCK REPAIR).

    Particoes que nao estao no DataFrame permanecem intocadas. Os tipos das colunas
    vem do schema PyArrow (ver pa_to_athena).

    Parametros:
        config (AthenaParquetConfig): configuracao da exportacao.

    Levanta (na criacao):
        ImportError: pyarrow nao instalado.
        ValueError: DataFrame vazio, coluna de particao inexistente, particao nula
            (com falhar_se_particao_nula=True), athena_results_prefix ausente ou
            dentro do LOCATION da tabela, ou drop_if_exists sem confirmar_drop.

    Requer:
        pyarrow e boto3, com permissao de s3:PutObject/DeleteObject/ListBucket,
        athena:StartQueryExecution/GetQueryExecution e glue:CreateTable/GetTable/BatchCreatePartition.

    Exemplo:
        >>> cfg = AthenaParquetConfig(
        ...     df=df, nome_parquet=None, bucket_name="meu-bucket",
        ...     pasta_no_bucket="dados/minha_tabela", database="meu_database",
        ...     tabela="minha_tabela", caminho_parquet_local="saida/minha_tabela.parquet",
        ...     profile_aws=None, workgroup="meu-workgroup", partition_cols=["dt"],
        ...     athena_results_prefix="resultados_athena/minha_tabela/")
        >>> ParquetAthenaExporter(cfg).run()
    """

    def __init__(self, config: AthenaParquetConfig):
        """Valida a configuracao, cria os clientes S3/Athena e prepara a tabela PyArrow.

        Parametros:
            config (AthenaParquetConfig): configuracao da exportacao.
        """
        self.config = config
        self.partition_cols = config.partition_cols or []
        self.pasta_no_bucket_limpa = config.pasta_no_bucket.strip("/")
        self.s3_location = f"s3://{config.bucket_name}/{self.pasta_no_bucket_limpa}/"

        # Sem prefixo padrao: este prefixo e APAGADO antes de cada DDL
        # (limpar_resultados_athena), entao precisa ser informado explicitamente.
        if not config.athena_results_prefix or not str(config.athena_results_prefix).strip("/"):
            raise ValueError(
                "Informe athena_results_prefix (prefixo exclusivo para resultados do Athena, "
                "ex.: 'resultados_athena/minha_tabela/'). Ele sera limpo a cada execucao."
            )
        self.athena_results_prefix = config.athena_results_prefix.strip("/") + "/"

        if pa is None:
            raise ImportError("pyarrow e necessario para ParquetAthenaExporter (pip install pyarrow).")
        if not config.nome_parquet:
            config.nome_parquet = config.tabela

        self._validar_configuracao_inicial()
        self.session = boto3.Session(profile_name=config.profile_aws)
        self.s3 = self.session.client("s3")
        self.athena = self.session.client("athena")
        self.df_export = self._preparar_dataframe_para_exportacao()
        self.table = pa.Table.from_pandas(self.df_export, preserve_index=False)
        self.schema = self.table.schema

        if self.partition_cols:
            base, _ = os.path.splitext(config.caminho_parquet_local)
            self.local_root_dir = base
            self.local_parquet_file = None
        else:
            self.local_root_dir = None
            self.local_parquet_file = config.caminho_parquet_local

    def _validar_configuracao_inicial(self):
        """Valida DROP sem confirmacao, DataFrame vazio, colunas de particao inexistentes e athena_results_prefix dentro do LOCATION.

        Levanta:
            ValueError: na primeira regra violada.
        """
        if self.config.drop_if_exists and not self.config.confirmar_drop:
            raise ValueError(
                "drop_if_exists=True pode remover o metadado da tabela Athena/Glue "
                "e impactar permissoes/acessos externos. "
                "Se realmente quiser executar DROP TABLE, informe confirmar_drop=True."
            )
        if self.config.df.empty:
            raise ValueError("O DataFrame informado esta vazio.")
        for col_part in self.partition_cols:
            if col_part not in self.config.df.columns:
                raise ValueError(
                    f"A coluna de particao '{col_part}' nao existe no DataFrame. "
                    f"Colunas disponiveis: {list(self.config.df.columns)}"
                )
        prefix_tabela = self.pasta_no_bucket_limpa.rstrip("/") + "/"
        prefix_resultado = self.athena_results_prefix.rstrip("/") + "/"
        if prefix_resultado.startswith(prefix_tabela):
            raise ValueError(
                "O prefixo de resultado do Athena esta dentro do LOCATION da tabela. "
                "Isso pode deixar arquivos CSV de resultado dentro do path Parquet "
                "e causar erro de leitura no Athena.\n"
                f"Prefixo da tabela: {prefix_tabela}\n"
                f"Prefixo Athena results: {prefix_resultado}"
            )

    def _preparar_dataframe_para_exportacao(self):
        """Retorna uma copia do DataFrame pronta para exportar: trata particoes nulas e, se configurado, converte particoes para texto.

        Retorna:
            pandas.DataFrame

        Levanta:
            ValueError: se houver particoes nulas e falhar_se_particao_nula=True.
        """
        df = self.config.df.copy()
        if not self.partition_cols:
            return df
        linhas_particao_nula = df[self.partition_cols].isna().any(axis=1)
        qtd_particao_nula = int(linhas_particao_nula.sum())
        if qtd_particao_nula > 0:
            msg = (
                f"Foram encontradas {qtd_particao_nula} linhas com valor nulo "
                f"em pelo menos uma coluna de particao: {self.partition_cols}."
            )
            if self.config.falhar_se_particao_nula:
                amostra = df.loc[linhas_particao_nula, self.partition_cols].head(20)
                raise ValueError(msg + "\nessas linhas impedem a criacao segura de particoes." + f"\nAmostra:\n{amostra}")
            print("Atencao:", msg)
            print("Essas linhas serao removidas antes da exportacao.")
            df = df.loc[~linhas_particao_nula].copy()
        if self.config.forcar_particoes_como_string:
            for col_part in self.partition_cols:
                df[col_part] = df[col_part].apply(normalizar_valor_particao)
        return df

    def salvar_parquet_local(self):
        """Grava o Parquet localmente: um arquivo unico, ou um dataset Hive em "<caminho sem extensao>/" quando ha particoes.

        Atencao: com particoes, a pasta local e apagada e recriada.
        """
        if self.partition_cols:
            print(f"Salvando dataset particionado em: {self.local_root_dir}")
            if os.path.exists(self.local_root_dir):
                shutil.rmtree(self.local_root_dir)
            os.makedirs(self.local_root_dir, exist_ok=True)
            ds.write_dataset(
                self.table,
                base_dir=self.local_root_dir,
                format="parquet",
                partitioning=self.partition_cols,
                partitioning_flavor="hive",
            )
        else:
            print(f"Salvando Parquet em: {self.local_parquet_file}")
            pasta_local = os.path.dirname(self.local_parquet_file)
            if pasta_local and not os.path.exists(pasta_local):
                os.makedirs(pasta_local, exist_ok=True)
            pq.write_table(self.table, self.local_parquet_file)

    def obter_particoes_afetadas(self):
        """Lista as combinacoes distintas de valores de particao presentes no DataFrame.

        Retorna:
            list[dict]: ex.: [{"dt": "2026-09-27"}, ...]. Lista vazia se nao houver particoes.

        Levanta:
            ValueError: se ha partition_cols, mas nenhuma combinacao valida.
        """
        if not self.partition_cols:
            return []
        df_part = self.df_export[self.partition_cols].dropna().drop_duplicates().copy()
        particoes = []
        for _, row in df_part.iterrows():
            item = {}
            for col_part in self.partition_cols:
                valor_formatado = normalizar_valor_particao(row[col_part])
                if valor_formatado is not None:
                    item[col_part] = valor_formatado
            if len(item) == len(self.partition_cols):
                particoes.append(item)
        if not particoes:
            raise ValueError("Nenhuma particao valida foi identificada no DataFrame.")
        print(f"Total de linhas a exportar: {len(self.df_export)}")
        print(f"Particoes afetadas identificadas: {particoes}")
        return particoes

    def montar_prefixo_particao(self, particao):
        """Monta o prefixo S3 de uma particao no padrao Hive: "<pasta_no_bucket>/col1=v1/col2=v2".

        Parametros:
            particao (dict): {coluna: valor} (item de obter_particoes_afetadas).

        Retorna:
            str
        """
        partes = [f"{col_part}={particao[col_part]}" for col_part in self.partition_cols]
        return self.pasta_no_bucket_limpa + "/" + "/".join(partes)

    def limpar_particoes_afetadas_s3(self):
        """Apaga no S3 apenas os prefixos das particoes que serao regravadas (se limpar_particoes_antes_upload=True)."""
        if not self.partition_cols:
            return
        if not self.config.limpar_particoes_antes_upload:
            print("Limpeza de particoes antes do upload desabilitada.")
            return
        particoes = self.obter_particoes_afetadas()
        for particao in particoes:
            prefixo = self.montar_prefixo_particao(particao)
            print(f"Limpando particao no S3: s3://{self.config.bucket_name}/{prefixo}")
            apagar_prefixo_s3_client(
                s3_client=self.s3,
                bucket_name=self.config.bucket_name,
                prefixo=prefixo,
            )

    def upload_para_s3(self):
        """Envia os arquivos .parquet gerados localmente para s3://<bucket_name>/<pasta_no_bucket>/, mantendo as subpastas de particao."""
        if self.partition_cols:
            print(f"Enviando arquivos particionados para S3 sob: {self.s3_location}")
            for root, _, files in os.walk(self.local_root_dir):
                for nome_arquivo in files:
                    if not nome_arquivo.endswith(".parquet"):
                        continue
                    local_path = os.path.join(root, nome_arquivo)
                    rel_path = os.path.relpath(local_path, self.local_root_dir).replace(os.sep, "/")
                    s3_key = f"{self.pasta_no_bucket_limpa}/{rel_path}"
                    print(f"Upload {local_path} -> s3://{self.config.bucket_name}/{s3_key}")
                    self.s3.upload_file(local_path, self.config.bucket_name, s3_key)
        else:
            chave_s3 = f"{self.pasta_no_bucket_limpa}/{self.config.nome_parquet}.parquet"
            print(f"Enviando arquivo para S3: s3://{self.config.bucket_name}/{chave_s3}")
            self.s3.upload_file(self.local_parquet_file, self.config.bucket_name, chave_s3)

    def montar_ddl(self):
        """Monta os trechos de colunas e de particoes do DDL a partir do schema PyArrow.

        Retorna:
            tuple[str, str]: (definicao das colunas comuns, definicao das colunas de particao; "" se nao houver).

        Levanta:
            ValueError: se todas as colunas forem de particao.
        """
        non_part_fields = [field for field in self.schema if field.name not in self.partition_cols]
        part_fields = [field for field in self.schema if field.name in self.partition_cols]
        if not non_part_fields:
            raise ValueError(
                "O schema ficou sem colunas comuns. "
                "Verifique se todas as colunas foram definidas como particao."
            )
        col_defs = [f"  `{field.name}` {pa_to_athena(field.type)}" for field in non_part_fields]
        ddl_colunas = ",\n".join(col_defs)
        if part_fields:
            # particoes foram normalizadas para texto (forcar_particoes_como_string)
            part_defs = [
                f"  `{field.name}` {'string' if self.config.forcar_particoes_como_string else pa_to_athena(field.type)}"
                for field in part_fields
            ]
            ddl_particoes = ", ".join(part_defs)
        else:
            ddl_particoes = ""
        return ddl_colunas, ddl_particoes

    def montar_query_create_table(self):
        """Monta o CREATE EXTERNAL TABLE IF NOT EXISTS (Parquet, SNAPPY) com LOCATION em pasta_no_bucket.

        Retorna:
            str: comando DDL.
        """
        ddl_colunas, ddl_particoes = self.montar_ddl()
        part_clause = f"\nPARTITIONED BY ({ddl_particoes})" if ddl_particoes else ""
        query = f"""CREATE EXTERNAL TABLE IF NOT EXISTS `{self.config.database}`.`{self.config.tabela}` (
{ddl_colunas}
){part_clause}
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '{self.s3_location}'
TBLPROPERTIES (
  'classification'='parquet',
  'parquet.compression'='SNAPPY'
)"""
        return query.strip()

    def limpar_resultados_athena(self):
        """Apaga os arquivos antigos em athena_results_prefix (resultados de execucoes anteriores)."""
        print(f"Limpando arquivos antigos de resultado Athena em: s3://{self.config.bucket_name}/{self.athena_results_prefix}")
        apagar_prefixo_s3_client(
            s3_client=self.s3,
            bucket_name=self.config.bucket_name,
            prefixo=self.athena_results_prefix,
        )

    def criar_tabela_athena(self):
        """Executa o DROP (se drop_if_exists e confirmar_drop) e o CREATE EXTERNAL TABLE IF NOT EXISTS. Tabela existente nao e alterada."""
        if self.config.drop_if_exists:
            drop_query = f"DROP TABLE IF EXISTS `{self.config.database}`.`{self.config.tabela}`"
            print(f"Executando DROP controlado: {drop_query}")
            self._executar_query(drop_query)
        query = self.montar_query_create_table()
        print("DDL que sera executado para garantir existencia da tabela:")
        print(query)
        self.limpar_resultados_athena()
        self._executar_query(query)
        print("Tabela criada ou ja existente.")

    def registrar_particoes_athena(self):
        """Registra cada particao do DataFrame com ALTER TABLE ... ADD IF NOT EXISTS PARTITION ... LOCATION."""
        if not self.partition_cols:
            return
        particoes = self.obter_particoes_afetadas()
        for particao in particoes:
            part_clause = ", ".join([f"`{col_part}`='{particao[col_part]}'" for col_part in self.partition_cols])
            location = f"s3://{self.config.bucket_name}/{self.montar_prefixo_particao(particao)}"
            query = f"""ALTER TABLE `{self.config.database}`.`{self.config.tabela}`
ADD IF NOT EXISTS
PARTITION ({part_clause})
LOCATION '{location}'"""
            print(f"Registrando particao no Athena: {particao}")
            print(query)
            self._executar_query(query)

    def reparar_particoes_athena(self):
        """Executa MSCK REPAIR TABLE (descobre todas as particoes no S3; mais lento em tabelas grandes)."""
        repair = f"MSCK REPAIR TABLE `{self.config.database}`.`{self.config.tabela}`"
        print(f"Atualizando metadados de particao com MSCK: {repair}")
        self._executar_query(repair)

    def _executar_query(self, query: str):
        """Executa uma query no Athena e aguarda a conclusao (consulta o status a cada 2 segundos).

        Parametros:
            query (str): comando SQL/DDL.

        Retorna:
            str: QueryExecutionId.

        Levanta:
            RuntimeError: se a query falhar ou for cancelada.
        """
        response = self.athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": self.config.database},
            ResultConfiguration={
                "OutputLocation": f"s3://{self.config.bucket_name}/{self.athena_results_prefix}"
            },
            WorkGroup=self.config.workgroup,
        )
        query_execution_id = response["QueryExecutionId"]

        while True:
            status_response = self.athena.get_query_execution(QueryExecutionId=query_execution_id)
            status = status_response["QueryExecution"]["Status"]["State"]
            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break
            time.sleep(2)

        if status == "SUCCEEDED":
            print(f"Query executada com sucesso. QueryExecutionId: {query_execution_id}")
            return query_execution_id
        if status == "FAILED":
            reason = status_response["QueryExecution"]["Status"].get("StateChangeReason", "Sem motivo informado")
            raise RuntimeError(
                f"Falha na execucao da query. QueryExecutionId: {query_execution_id}. Motivo: {reason}"
            )
        raise RuntimeError(f"Query foi cancelada. QueryExecutionId: {query_execution_id}.")

    def run(self):
        """Executa o fluxo completo: Parquet local -> limpeza das particoes -> upload -> CREATE TABLE -> registro das particoes.

        Retorna:
            None
        """
        self.salvar_parquet_local()
        if self.partition_cols:
            self.limpar_particoes_afetadas_s3()
        self.upload_para_s3()
        self.criar_tabela_athena()
        if self.partition_cols:
            if self.config.usar_msck:
                self.reparar_particoes_athena()
            else:
                self.registrar_particoes_athena()
        print(f"Processo concluido com sucesso para {self.config.database}.{self.config.tabela}.")


def criar_tabela_athena_parquet(
    df, bucket_name, pasta_no_bucket, database, tabela, profile_aws, workgroup,
    caminho_parquet_local, drop_if_exists=False, confirmar_drop=False,
    partition_cols=None, usar_msck=False, athena_results_prefix=None,
):
    """Atalho para exportar um DataFrame pandas para uma tabela Athena com ParquetAthenaExporter.

    Usa limpar_particoes_antes_upload=True, falhar_se_particao_nula=True e
    forcar_particoes_como_string=True. Para outras opcoes, use AthenaParquetConfig
    diretamente.

    Parametros:
        df (pandas.DataFrame): dados a exportar.
        bucket_name (str): bucket de destino.
        pasta_no_bucket (str): prefixo S3 da tabela (LOCATION).
        database (str): database do Athena/Glue.
        tabela (str): nome da tabela.
        profile_aws (str | None): perfil do ~/.aws/config; None usa as credenciais padrao.
        workgroup (str): workgroup do Athena.
        caminho_parquet_local (str): arquivo .parquet local temporario.
        drop_if_exists (bool, opcional): recria a tabela. Padrao: False.
        confirmar_drop (bool, opcional): obrigatorio junto com drop_if_exists. Padrao: False.
        partition_cols (list[str], opcional): colunas de particao.
        usar_msck (bool, opcional): usa MSCK REPAIR TABLE. Padrao: False.
        athena_results_prefix (str): prefixo S3 para resultados do Athena
            (obrigatorio; e apagado a cada execucao).

    Retorna:
        tuple: ("SUCCEEDED", None) quando conclui.

    Levanta:
        ValueError, ImportError, RuntimeError: ver ParquetAthenaExporter.

    Exemplo:
        >>> criar_tabela_athena_parquet(
        ...     df, "meu-bucket", "dados/minha_tabela", "meu_database", "minha_tabela",
        ...     profile_aws=None, workgroup="meu-workgroup",
        ...     caminho_parquet_local="saida/minha_tabela.parquet",
        ...     partition_cols=["dt"], athena_results_prefix="resultados_athena/minha_tabela/")
    """
    config = AthenaParquetConfig(
        df=df,
        nome_parquet=None,
        bucket_name=bucket_name,
        pasta_no_bucket=pasta_no_bucket,
        database=database,
        tabela=tabela,
        caminho_parquet_local=caminho_parquet_local,
        profile_aws=profile_aws,
        workgroup=workgroup,
        drop_if_exists=drop_if_exists,
        confirmar_drop=confirmar_drop,
        partition_cols=partition_cols,
        limpar_particoes_antes_upload=True,
        usar_msck=usar_msck,
        athena_results_prefix=athena_results_prefix,
        falhar_se_particao_nula=True,
        forcar_particoes_como_string=True,
    )
    exporter = ParquetAthenaExporter(config)
    exporter.run()
    return "SUCCEEDED", None
