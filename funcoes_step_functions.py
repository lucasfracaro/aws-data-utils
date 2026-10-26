"""funcoes_step_functions - Execucao sequencial de cargas em uma AWS Step Function.

Cada item de uma lista de payloads e enviado como input de uma nova execucao;
a proxima so comeca quando a anterior termina com sucesso.

Credenciais e regiao seguem a cadeia padrao do boto3 (variaveis de ambiente,
~/.aws/config, role do Glue/Lambda etc.).
"""

import boto3
import json
import re
import time
from datetime import datetime
import pandas as pd


# ============================================================
# 11. STEP FUNCTIONS
# ============================================================

def executar_step_function_sequencial(nome_step_function, execucoes, intervalo_segundos=30):
    """Executa uma lista de payloads em uma Step Function, um apos o outro, aguardando cada execucao terminar.

    A execucao e interrompida na primeira falha: o erro e impresso e a excecao
    e relancada (as execucoes seguintes nao sao iniciadas).

    Parametros:
        nome_step_function (str): nome da state machine (nao o ARN).
        execucoes (list[dict]): payloads; cada dict vira o input JSON de uma execucao.
            As chaves "tablename" (ou "ator", "segmento", "fluxo", "item",
            "frequencia") sao usadas para nomear a execucao; "dataset",
            "queryservice" e "glue_tier" sao copiadas para o resultado, se existirem.
        intervalo_segundos (int, opcional): intervalo entre consultas de status. Padrao: 30.

    Retorna:
        pandas.DataFrame: uma linha por execucao concluida, com as colunas dataset,
        objeto, queryservice, glue_tier, nome_execucao, execution_arn, status,
        startDate, stopDate, duracao_segundos e output.

    Levanta:
        ValueError: se a Step Function nao for encontrada.
        RuntimeError: se uma execucao terminar como FAILED, TIMED_OUT ou ABORTED.

    Exemplo:
        >>> df = executar_step_function_sequencial(
        ...     "minha-state-machine",
        ...     [{"tablename": "clientes"}, {"tablename": "contratos"}],
        ...     intervalo_segundos=15,
        ... )
    """
    resultados = []
    client_sf = criar_client_stepfunctions()
    state_machine_arn = obter_state_machine_arn_por_nome(nome_step_function, client_sf)

    for parametros in execucoes:
        execution_arn = None
        nome_execucao = None
        print()
        print("=" * 100)
        nome_execucao_log = obter_nome_objeto(parametros)
        print(f"Executando {nome_execucao_log}")
        print("=" * 100)

        try:
            execution_arn, nome_execucao = iniciar_execucao(
                state_machine_arn=state_machine_arn,
                parametros=parametros,
            )
            resultado = aguardar_execucao(
                execution_arn=execution_arn,
                intervalo_segundos=intervalo_segundos,
            )
            start_date = resultado.get("startDate")
            stop_date = resultado.get("stopDate")
            duracao_segundos = None
            if start_date and stop_date:
                duracao_segundos = (stop_date - start_date).total_seconds()

            resultados.append({
                "dataset": parametros.get("dataset"),
                "objeto": obter_nome_objeto(parametros),
                "queryservice": parametros.get("queryservice"),
                "glue_tier": parametros.get("glue_tier"),
                "nome_execucao": nome_execucao,
                "execution_arn": execution_arn,
                "status": resultado.get("status"),
                "startDate": start_date,
                "stopDate": stop_date,
                "duracao_segundos": duracao_segundos,
                "output": resultado.get("output"),
            })
        except Exception as e:
            print("=" * 100)
            print(str(e))
            resultados.append({
                "dataset": parametros.get("dataset"),
                "objeto": obter_nome_objeto(parametros),
                "queryservice": parametros.get("queryservice"),
                "glue_tier": parametros.get("glue_tier"),
                "nome_execucao": nome_execucao,
                "execution_arn": execution_arn,
                "status": "ERRO",
                "startDate": None,
                "stopDate": None,
                "duracao_segundos": None,
                "output": None,
                "erro": str(e),
            })
            raise

    print("=" * 100)
    print("Todas as execucoes finalizaram com sucesso.")
    print("=" * 100)
    return pd.DataFrame(resultados)


def criar_client_stepfunctions():
    """Cria um cliente boto3 de Step Functions com a configuracao padrao de credenciais/regiao.

    Retorna:
        botocore.client.SFN
    """
    return boto3.client("stepfunctions")


def obter_state_machine_arn_por_nome(nome_step_function: str, client_sf=None) -> str:
    """Localiza o ARN de uma Step Function a partir do nome, percorrendo todas as paginas de list_state_machines.

    Parametros:
        nome_step_function (str): nome exato da state machine.
        client_sf (botocore.client.SFN, opcional): cliente ja criado. Se None, cria um.

    Retorna:
        str: ARN da state machine.

    Levanta:
        ValueError: se nenhuma state machine tiver esse nome.
    """
    if client_sf is None:
        client_sf = criar_client_stepfunctions()
    paginator = client_sf.get_paginator("list_state_machines")
    for page in paginator.paginate():
        for sm in page.get("stateMachines", []):
            if sm.get("name") == nome_step_function:
                return sm.get("stateMachineArn")
    raise ValueError(f"Step Function nao encontrada: {nome_step_function}")


def iniciar_execucao(state_machine_arn, parametros):
    """Inicia uma execucao da Step Function com o payload informado.

    O nome da execucao e gerado por gerar_nome_execucao (nome do objeto + data/hora).

    Parametros:
        state_machine_arn (str): ARN da state machine.
        parametros (dict): input da execucao. Valores nao serializaveis em JSON
            (datas, Decimal...) sao convertidos com str().

    Retorna:
        tuple[str, str]: (execution_arn, nome_execucao).
    """
    client_sf = boto3.client("stepfunctions")
    nome_execucao_log = obter_nome_objeto(parametros)
    nome_execucao = gerar_nome_execucao(nome_execucao_log)
    print("=" * 100)
    print(f"Iniciando execucao: {nome_execucao}")
    print(f"objeto: {nome_execucao_log}")
    response = client_sf.start_execution(
        stateMachineArn=state_machine_arn,
        name=nome_execucao,
        input=json.dumps(parametros, default=str),
    )
    execution_arn = response["executionArn"]
    print(f"Execution ARN: {execution_arn}")
    return execution_arn, nome_execucao


def aguardar_execucao(execution_arn, intervalo_segundos=30):
    """Consulta o status de uma execucao ate ela terminar.

    Parametros:
        execution_arn (str): ARN da execucao.
        intervalo_segundos (int, opcional): espera entre consultas. Padrao: 30.

    Retorna:
        dict: resposta de describe_execution (inclui status, startDate, stopDate, output)
        quando o status for SUCCEEDED.

    Levanta:
        RuntimeError: se o status final for FAILED, TIMED_OUT ou ABORTED
            (a mensagem inclui o "error" e a "cause" da execucao).
    """
    client_sf = boto3.client("stepfunctions")
    while True:
        response = client_sf.describe_execution(executionArn=execution_arn)
        status = response["status"]
        print(f"Status atual: {status}")
        if status == "SUCCEEDED":
            print("Execucao finalizada com sucesso.")
            output = response.get("output")
            if output:
                print("output:")
                print(output)
            return response
        if status in ["FAILED", "TIMED_OUT", "ABORTED"]:
            erro = response.get("error", "")
            causa = response.get("cause", "")
            print("=" * 100)
            print("STEP FUNCTION FALHOU")
            print("=" * 100)
            print(f"Status: {status}")
            print(f"Erro: {erro}")
            print(f"Causa: {causa}")
            print("=" * 100)
            raise RuntimeError(f"Execucao terminou com status {status}.\nErro: {erro}\nCausa: {causa}")
        time.sleep(intervalo_segundos)


def gerar_nome_execucao(nome_base):
    """Gera um nome unico e valido para uma execucao de Step Function: "<nome_base>_YYYYMMDD_HHMMSS".

    A Step Functions aceita no maximo 80 caracteres, apenas letras, numeros, "-" e
    "_". Qualquer outro caractere e trocado por "_" e o nome base e truncado para
    caber no limite.

    Parametros:
        nome_base (str): texto base (ex.: nome da tabela). Se vazio, usa "execucao".

    Retorna:
        str

    Exemplo:
        >>> gerar_nome_execucao("minha.tabela")   # doctest: +SKIP
        'minha_tabela_20260927_101500'
    """
    sufixo_data = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = re.sub(r"[^A-Za-z0-9_-]", "_", str(nome_base or "execucao")).strip("_") or "execucao"
    base = base[: 80 - len(sufixo_data) - 1]
    return f"{base}_{sufixo_data}"


def obter_nome_objeto(parametros):
    """Obtem um nome legivel para uma execucao a partir do payload.

    Usa "tablename"; se nao existir, junta com "_" os valores preenchidos de
    "ator", "segmento", "fluxo", "item" e "frequencia".

    Parametros:
        parametros (dict): payload da execucao.

    Retorna:
        str: nome do objeto (pode ser "" se nenhuma dessas chaves existir).
    """
    return (
        parametros.get("tablename")
        or "_".join(
            str(v)
            for v in [
                parametros.get("ator"),
                parametros.get("segmento"),
                parametros.get("fluxo"),
                parametros.get("item"),
                parametros.get("frequencia"),
            ]
            if v is not None and str(v).strip() != ""
        )
    )
