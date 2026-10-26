"""funcoes_glue - Monitoramento de execucoes de AWS Glue Jobs.

Busca as execucoes de um job em um dia, identifica quais correspondem a cada
carga (pelos argumentos/payload), le os logs no CloudWatch e classifica cada
execucao em POSITIVO, FALSO_POSITIVO, ALERTA, FRACASSO, EM_EXECUCAO ou
INDETERMINADO. Tambem traz utilitarios para interpretar mensagens de erro.

Funcoes principais:
    avaliar_job_glue_por_lista_payloads_nomeados  varias cargas nomeadas de um job
    avaliar_job_glue_visual                       idem, com colunas amigaveis de erro
    avaliar_job_glue_por_payload                  todas as execucoes que casam com um filtro
    avaliar_ultima_execucao_glue_por_payload      so a execucao mais recente

Permissoes AWS necessarias: glue:GetJobRuns, glue:GetJobRun e logs:GetLogEvents.
"""

import boto3
import pandas as pd
import re
from typing import Dict, Optional, List, Any

from datas import normalizar_data_referencia
from utils_gerais import valor_vazio, normalizar_valor_particao, print_df

STATUS_PROBLEMA = ["FALSO_POSITIVO", "FRACASSO", "ALERTA", "INDETERMINADO"]


# ============================================================
# 12. GLUE - AVALIACAO DE EXECUCOES
# ============================================================

def obter_execucoes_glue(nome_job, data_referencia=None, timezone="America/Sao_Paulo", max_runs=None):
    """Lista as execucoes de um Glue Job iniciadas em um determinado dia.

    Percorre get_job_runs (do mais recente para o mais antigo) e para assim que
    encontra execucoes anteriores ao dia pedido.

    Parametros:
        nome_job (str): nome do Glue Job.
        data_referencia (None | date | datetime | str, opcional): dia a avaliar
            ("YYYY-MM-DD" ou "YYYYMMDD"). Padrao: hoje no fuso "timezone".
        timezone (str, opcional): fuso usado para decidir o dia de cada execucao.
            Padrao: "America/Sao_Paulo".
        max_runs (int, opcional): para de buscar apos esse numero de execucoes do dia.

    Retorna:
        pandas.DataFrame: uma linha por execucao, ordenado da mais recente para a mais
        antiga, com as colunas job_name, job_run_id, status, started_on,
        completed_on, execution_time, dpu_seconds, worker_type, number_of_workers,
        attempt, error_message, log_group_name, trigger_name, glue_version,
        execution_class e arguments (dict). Vazio se nao houver execucoes no dia.

    Exemplo:
        >>> df = obter_execucoes_glue("meu_job", "2026-09-27")
        >>> df[["job_run_id", "status", "started_on"]]
    """
    glue = boto3.client("glue")
    data_ref = normalizar_data_referencia(data_referencia=data_referencia, timezone=timezone)
    tz = __import__("zoneinfo").ZoneInfo(timezone)
    paginator = glue.get_paginator("get_job_runs")
    registros = []

    for page in paginator.paginate(JobName=nome_job):
        for run in page.get("JobRuns", []):
            started_on = run.get("StartedOn")
            if started_on is None:
                continue
            data_execucao = started_on.astimezone(tz).date()
            # get_job_runs retorna do mais recente para o mais antigo
            if data_execucao < data_ref:
                df = pd.DataFrame(registros)
                if not df.empty:
                    df = df.sort_values("started_on", ascending=False).reset_index(drop=True)
                return df
            if data_execucao > data_ref:
                # execucoes de dias posteriores ao pedido
                continue
            registros.append({
                "job_name": nome_job,
                "job_run_id": run.get("Id"),
                "status": run.get("JobRunState"),
                "started_on": started_on,
                "completed_on": run.get("CompletedOn"),
                "execution_time": run.get("ExecutionTime"),
                "dpu_seconds": run.get("DPUSeconds"),
                "worker_type": run.get("WorkerType"),
                "number_of_workers": run.get("NumberOfWorkers"),
                "attempt": run.get("Attempt"),
                "error_message": run.get("ErrorMessage"),
                "log_group_name": run.get("LogGroupName"),
                "trigger_name": run.get("TriggerName"),
                "glue_version": run.get("GlueVersion"),
                "execution_class": run.get("ExecutionClass"),
                "arguments": run.get("Arguments", {}),
            })
            if max_runs and len(registros) >= max_runs:
                df = pd.DataFrame(registros)
                if not df.empty:
                    df = df.sort_values("started_on", ascending=False).reset_index(drop=True)
                return df

    df = pd.DataFrame(registros)
    if not df.empty:
        df = df.sort_values("started_on", ascending=False).reset_index(drop=True)
    return df


def expandir_argumentos_glue(df):
    """Transforma a coluna "arguments" (dict) em colunas, removendo o prefixo "--" dos nomes.

    Parametros:
        df (pandas.DataFrame): resultado de obter_execucoes_glue.

    Retorna:
        pandas.DataFrame: copia sem a coluna "arguments" e com uma coluna por
        argumento (ex.: "--tablename" vira "tablename"). Vazio se df for vazio/None.
    """
    if df is None or df.empty:
        return pd.DataFrame()
    if "arguments" not in df.columns:
        return df.copy()
    df_base = df.copy()
    argumentos = df_base["arguments"].apply(lambda x: x if isinstance(x, dict) else {}).apply(pd.Series)
    argumentos.columns = [str(col).replace("--", "") for col in argumentos.columns]
    return pd.concat([df_base.drop(columns=["arguments"]), argumentos], axis=1)


def filtrar_execucoes_por_payload(df, filtro_input=None, chaves_comparacao=None,
                                   ignorar_chaves_vazias_no_run=True, retornar_diagnostico_filtro=True):
    """Mantem apenas as execucoes cujos argumentos correspondem a um payload (ou a pelo menos um de uma lista).

    Os payloads sao comparados com os argumentos de cada execucao, ja sem o
    prefixo "--" (ex.: {"tablename": "clientes"} casa com "--tablename clientes").
    Os valores sao comparados como texto normalizado (20260927.0 == "20260927").

    Parametros:
        df (pandas.DataFrame): execucoes ja expandidas (expandir_argumentos_glue).
        filtro_input (None | dict | list[dict], opcional): payload(s) de filtro.
            None devolve todas as execucoes.
        chaves_comparacao (list[str], opcional): compara apenas estas chaves do
            payload. Padrao: todas as chaves preenchidas do payload.
        ignorar_chaves_vazias_no_run (bool, opcional): se True, uma chave do payload
            ausente (ou vazia) nos argumentos da execucao e ignorada, em vez de
            eliminar a execucao. Padrao: True.
        retornar_diagnostico_filtro (bool, opcional): adiciona as colunas de
            diagnostico abaixo. Padrao: True.

    Retorna:
        pandas.DataFrame: execucoes que casaram. Com diagnostico, inclui:
            _payload_match (bool), _payload_index (posicao do payload que casou),
            _filtros_aplicados ({chave: valor} efetivamente comparados),
            _filtros_ignorados e _filtros_divergentes (motivos, para depuracao).

    Levanta:
        ValueError: se filtro_input nao for None, dict ou lista.
    """
    if df is None or df.empty:
        return pd.DataFrame()
    if filtro_input is None:
        df_retorno = df.copy()
        if retornar_diagnostico_filtro:
            df_retorno["_payload_match"] = True
            df_retorno["_payload_index"] = None
            df_retorno["_filtros_aplicados"] = None
            df_retorno["_filtros_ignorados"] = None
            df_retorno["_filtros_divergentes"] = None
        return df_retorno

    if isinstance(filtro_input, dict):
        filtros = [filtro_input]
    elif isinstance(filtro_input, list):
        filtros = filtro_input
    else:
        raise ValueError("filtro_input deve ser None, dict ou list[dict].")

    registros_match = []
    for _, row in df.iterrows():
        encontrou_match = False
        payload_index_match = None
        filtros_aplicados_match = None
        filtros_ignorados_match = None
        filtros_divergentes_match = None

        for pos_payload, payload in enumerate(filtros):
            filtros_aplicados = {}
            filtros_ignorados = {}
            filtros_divergentes = {}

            if chaves_comparacao is None:
                chaves = [chave for chave, valor in payload.items() if not valor_vazio(valor)]
            else:
                chaves = [chave for chave in chaves_comparacao if chave in payload and not valor_vazio(payload.get(chave))]

            payload_valido = True
            for chave in chaves:
                valor_esperado = normalizar_valor_particao(payload.get(chave))
                if chave not in df.columns:
                    if ignorar_chaves_vazias_no_run:
                        filtros_ignorados[chave] = {"motivo": "coluna_ausente_no_dataframe", "valor_payload": valor_esperado}
                        continue
                    payload_valido = False
                    filtros_divergentes[chave] = {"valor_payload": valor_esperado, "valor_run": None, "motivo": "coluna_ausente_no_dataframe"}
                    break
                valor_run = normalizar_valor_particao(row.get(chave))
                if valor_run is None:
                    if ignorar_chaves_vazias_no_run:
                        filtros_ignorados[chave] = {"motivo": "valor_vazio_no_run", "valor_payload": valor_esperado, "valor_run": valor_run}
                        continue
                    payload_valido = False
                    filtros_divergentes[chave] = {"valor_payload": valor_esperado, "valor_run": valor_run, "motivo": "valor_vazio_no_run"}
                    break
                if valor_run != valor_esperado:
                    payload_valido = False
                    filtros_divergentes[chave] = {"valor_payload": valor_esperado, "valor_run": valor_run, "motivo": "valor_diferente"}
                    break
                # usado por exigir_ao_menos_um_filtro_aplicado
                filtros_aplicados[chave] = valor_esperado

            if payload_valido:
                encontrou_match = True
                payload_index_match = pos_payload
                filtros_aplicados_match = filtros_aplicados
                filtros_ignorados_match = filtros_ignorados
                filtros_divergentes_match = filtros_divergentes
                break

        if encontrou_match:
            registro = row.to_dict()
            if retornar_diagnostico_filtro:
                registro["_payload_match"] = True
                registro["_payload_index"] = payload_index_match
                registro["_filtros_aplicados"] = filtros_aplicados_match
                registro["_filtros_ignorados"] = filtros_ignorados_match
                registro["_filtros_divergentes"] = filtros_divergentes_match
            registros_match.append(registro)

    return pd.DataFrame(registros_match)


def filtrar_execucoes_por_lista_payloads_nomeados(
    df_execucoes, lista_payloads=None, chaves_comparacao=None,
    ignorar_chaves_vazias_no_run=True, retornar_diagnostico_filtro=True,
    exigir_ao_menos_um_filtro_aplicado=True, permitir_multiplos_matches=True,
):
    """Associa execucoes do Glue a cargas nomeadas: cada execucao recebe o "name" do payload que ela atende.

    Os payloads sao comparados com os argumentos de cada execucao, ja sem o
    prefixo "--" (ex.: {"tablename": "clientes"} casa com "--tablename clientes").
    Os valores sao comparados como texto normalizado (20260927.0 == "20260927").

    Parametros:
        df_execucoes (pandas.DataFrame): execucoes ja expandidas (expandir_argumentos_glue).
        lista_payloads (list[tuple[str, dict]] | None): [(nome, payload), ...].
            None ou lista vazia devolve todas as execucoes com name="all".
        chaves_comparacao (list[str], opcional): compara apenas estas chaves do
            payload. Padrao: todas as chaves preenchidas do payload.
        ignorar_chaves_vazias_no_run (bool, opcional): se True, uma chave do payload
            ausente (ou vazia) nos argumentos da execucao e ignorada, em vez de
            eliminar a execucao. Padrao: True.
        retornar_diagnostico_filtro (bool, opcional): inclui colunas de diagnostico
            (ver filtrar_execucoes_por_payload) e _payload_name. Padrao: True.
        exigir_ao_menos_um_filtro_aplicado (bool, opcional): descarta execucoes em
            que nenhuma chave do payload foi de fato comparada (ex.: todas
            ignoradas por ausencia). Padrao: True.
        permitir_multiplos_matches (bool, opcional): se False, cada execucao fica
            so com o primeiro payload que a atender. Padrao: True.

    Retorna:
        pandas.DataFrame: com a coluna "name" primeiro, ordenado por name e
        started_on (mais recente primeiro). Vazio se nada casar.

    Levanta:
        ValueError: se lista_payloads estiver fora do formato [(nome, dict), ...].
    """
    if df_execucoes is None or df_execucoes.empty:
        return pd.DataFrame()
    df_base = df_execucoes.copy()
    payloads_nomeados = normalizar_lista_payloads_nomeados(lista_payloads=lista_payloads)

    if payloads_nomeados is None:
        df_saida = df_base.copy()
        df_saida.insert(0, "name", "all")
        if retornar_diagnostico_filtro:
            df_saida["_payload_match"] = True
            df_saida["_payload_index"] = None
            df_saida["_payload_name"] = "all"
            df_saida["_filtros_aplicados"] = None
            df_saida["_filtros_ignorados"] = None
            df_saida["_filtros_divergentes"] = None
        return reordenar_coluna_name_primeira(df_saida)

    registros = []
    job_runs_ja_classificados = set()
    for pos_payload, (name, payload) in enumerate(payloads_nomeados):
        df_match = filtrar_execucoes_por_payload(
            df=df_execucoes, filtro_input=payload, chaves_comparacao=chaves_comparacao,
            ignorar_chaves_vazias_no_run=ignorar_chaves_vazias_no_run,
            retornar_diagnostico_filtro=retornar_diagnostico_filtro,
        )
        if df_match is None or df_match.empty:
            continue
        for _, row in df_match.iterrows():
            job_run_id = row.get("job_run_id")
            if not permitir_multiplos_matches and job_run_id in job_runs_ja_classificados:
                continue
            filtros_aplicados = row.get("_filtros_aplicados")
            if exigir_ao_menos_um_filtro_aplicado:
                qt_filtros_aplicados = 0
                if isinstance(filtros_aplicados, dict):
                    qt_filtros_aplicados = len(filtros_aplicados)
                if qt_filtros_aplicados == 0:
                    continue
            registro = row.to_dict()
            registro["name"] = name
            if retornar_diagnostico_filtro:
                registro["_payload_index"] = pos_payload
                registro["_payload_name"] = name
            registros.append(registro)
            if job_run_id is not None:
                job_runs_ja_classificados.add(job_run_id)

    if not registros:
        return pd.DataFrame()
    df_saida = pd.DataFrame(registros)
    if "started_on" in df_saida.columns:
        df_saida = df_saida.sort_values(["name", "started_on"], ascending=[True, False]).reset_index(drop=True)
    return reordenar_coluna_name_primeira(df_saida)


def normalizar_lista_payloads_nomeados(lista_payloads=None):
    """Valida e padroniza uma lista de payloads nomeados.

    Parametros:
        lista_payloads (list[tuple[str, dict]] | None): [(nome, payload), ...].

    Retorna:
        list[tuple[str, dict]] | None: a lista com nomes sem espacos nas pontas,
        ou None se a entrada for None ou vazia.

    Levanta:
        ValueError: se nao for lista, se algum item nao for tupla de 2 elementos,
            se o nome for vazio ou se o payload nao for dict.

    Exemplo:
        >>> normalizar_lista_payloads_nomeados([(" Clientes ", {"tablename": "clientes"})])
        [('Clientes', {'tablename': 'clientes'})]
    """
    if lista_payloads is None:
        return None
    if not isinstance(lista_payloads, list):
        raise ValueError("lista_payloads deve ser None ou uma lista de tuplas no formato: [(name, payload_dict), ...]")
    if len(lista_payloads) == 0:
        return None
    lista_normalizada = []
    for pos, item in enumerate(lista_payloads):
        if not isinstance(item, tuple) or len(item) != 2:
            raise ValueError(f"Item na posicao {pos} invalido. Cada item deve ser uma tupla: (name, payload_dict).")
        name, payload = item
        if valor_vazio(name):
            raise ValueError(f"Nome vazio na posicao {pos}. O primeiro item da tupla deve ser o nome legivel do negocio.")
        if not isinstance(payload, dict):
            raise ValueError(f"Payload invalido na posicao {pos}. O segundo item da tupla deve um dicionario.")
        lista_normalizada.append((str(name).strip(), payload))
    return lista_normalizada


def manter_ultima_execucao_por_name(df_correlacionado):
    """Mantem apenas a execucao mais recente (maior started_on) de cada "name".

    Parametros:
        df_correlacionado (pandas.DataFrame): resultado de
            filtrar_execucoes_por_lista_payloads_nomeados. Sem a coluna "name",
            todas as linhas recebem name="all".

    Retorna:
        pandas.DataFrame: uma linha por name, com "name" como primeira coluna.
    """
    if df_correlacionado is None or df_correlacionado.empty:
        return pd.DataFrame()
    df_base = df_correlacionado.copy()
    if "name" not in df_base.columns:
        df_base.insert(0, "name", "all")
    if "started_on" not in df_base.columns:
        return reordenar_coluna_name_primeira(df_base.drop_duplicates(subset="name", keep="first").reset_index(drop=True))
    df_saida = (
        df_base
        .sort_values(["name", "started_on"], ascending=[True, False])
        .groupby("name", as_index=False)
        .head(1)
        .reset_index(drop=True)
    )
    return reordenar_coluna_name_primeira(df_saida)


def reordenar_coluna_name_primeira(df):
    """Move a coluna "name" para a primeira posicao, se existir.

    Parametros:
        df (pandas.DataFrame | None): DataFrame de entrada.

    Retorna:
        pandas.DataFrame | None: o mesmo conteudo com "name" primeiro (inalterado se nao houver "name").
    """
    if df is None or df.empty:
        return df
    if "name" not in df.columns:
        return df
    colunas = ["name"] + [coluna for coluna in df.columns if coluna != "name"]
    return df[colunas]


def montar_resumo_avaliacao_nomeada(nome_job, data_referencia, df_execucoes, df_correlacionado, df_avaliacao, last_execution):
    """Monta o resumo geral e o resumo por "name" de uma avaliacao de execucoes.

    Parametros:
        nome_job (str): nome do Glue Job.
        data_referencia (date): dia avaliado.
        df_execucoes (pandas.DataFrame): todas as execucoes do dia.
        df_correlacionado (pandas.DataFrame): execucoes associadas a algum payload.
        df_avaliacao (pandas.DataFrame): execucoes avaliadas (com resultado_operacional).
        last_execution (bool): se foi considerada so a ultima execucao por name.

    Retorna:
        tuple[dict, pandas.DataFrame]:
            resumo: nome_job, data_referencia, last_execution, total_execucoes_data,
                total_execucoes_correlacionadas, total_execucoes_avaliadas, positivos,
                falsos_positivos, fracassos, alertas, em_execucao, indeterminados;
            df_resumo_name: por name, as colunas total, positivos, falsos_positivos,
                fracassos, alertas, em_execucao e indeterminados.
    """
    # mesmas chaves do resumo devolvido quando nao ha execucoes
    resumo = {
        "nome_job": nome_job,
        "data_referencia": data_referencia,
        "last_execution": last_execution,
        "total_execucoes_data": 0 if df_execucoes is None else len(df_execucoes),
        "total_execucoes_correlacionadas": 0 if df_correlacionado is None else len(df_correlacionado),
        "total_execucoes_avaliadas": 0 if df_avaliacao is None else len(df_avaliacao),
        "positivos": 0,
        "falsos_positivos": 0,
        "fracassos": 0,
        "alertas": 0,
        "em_execucao": 0,
        "indeterminados": 0,
    }
    if df_avaliacao is not None and not df_avaliacao.empty:
        resumo["positivos"] = int((df_avaliacao["resultado_operacional"] == "POSITIVO").sum())
        resumo["falsos_positivos"] = int((df_avaliacao["resultado_operacional"] == "FALSO_POSITIVO").sum())
        resumo["fracassos"] = int((df_avaliacao["resultado_operacional"] == "FRACASSO").sum())
        resumo["alertas"] = int((df_avaliacao["resultado_operacional"] == "ALERTA").sum())
        resumo["em_execucao"] = int((df_avaliacao["resultado_operacional"] == "EM_EXECUCAO").sum())
        resumo["indeterminados"] = int((df_avaliacao["resultado_operacional"] == "INDETERMINADO").sum())

    if df_avaliacao is None or df_avaliacao.empty or "name" not in df_avaliacao.columns:
        df_resumo_name = pd.DataFrame(columns=["name", "total", "positivos", "falsos_positivos", "fracassos", "alertas", "em_execucao", "indeterminados"])
    else:
        df_resumo_name = (
            df_avaliacao
            .assign(
                positivo=lambda x: x["resultado_operacional"].eq("POSITIVO"),
                falso_positivo=lambda x: x["resultado_operacional"].eq("FALSO_POSITIVO"),
                fracasso=lambda x: x["resultado_operacional"].eq("FRACASSO"),
                alerta=lambda x: x["resultado_operacional"].eq("ALERTA"),
                em_execucao_calc=lambda x: x["resultado_operacional"].eq("EM_EXECUCAO"),
                indeterminado=lambda x: x["resultado_operacional"].eq("INDETERMINADO"),
            )
            .groupby("name", as_index=False)
            .agg(
                total=("job_run_id", "count"),
                positivos=("positivo", "sum"),
                falsos_positivos=("falso_positivo", "sum"),
                fracassos=("fracasso", "sum"),
                alertas=("alerta", "sum"),
                em_execucao=("em_execucao_calc", "sum"),
                indeterminados=("indeterminado", "sum"),
            )
        )
    return resumo, df_resumo_name


def avaliar_execucao_glue(row_execucao, avaliar_Logs_em_sucesso=True, avaliar_Logs_em_falha=True):
    """Classifica uma unica execucao do Glue, lendo o log no CloudWatch quando necessario.

    Classificacao (coluna "resultado_operacional"):
        POSITIVO        SUCCEEDED e sem erro/aviso no log (ou log nao avaliado).
        FALSO_POSITIVO  SUCCEEDED, mas o log contem mensagens de erro.
        ALERTA          SUCCEEDED, com avisos (WARN) no log.
        FRACASSO        FAILED, TIMEOUT, STOPPED, ERROR ou EXPIRED.
        EM_EXECUCAO     STARTING, RUNNING, WAITING ou STOPPING.
        INDETERMINADO   status desconhecido ou falha ao avaliar a execucao/logs.

    Parametros:
        row_execucao (pandas.Series | dict): uma linha de obter_execucoes_glue
            (usa job_name, job_run_id, status e error_message).
        avaliar_Logs_em_sucesso (bool, opcional): le o log das execucoes SUCCEEDED
            para detectar FALSO_POSITIVO/ALERTA. Padrao: True.
        avaliar_Logs_em_falha (bool, opcional): le o log das execucoes com falha
            quando a AWS nao informa ErrorMessage. Padrao: True.

    Retorna:
        dict com: job_run_id, resultado_operacional, status_operacional (ex.:
        "SUCCEEDED_WITH_ERRORS"), log_localizado, qt_linhas_log, qt_erros_log,
        qt_warnings_log, possui_erro_log, possui_warning_log, erro_principal,
        erro_principal_limpo, objeto_erro, timestamp_erro_principal, detalhe_erro
        e, quando o log e lido, status_log.
    """
    job_name = row_execucao.get("job_name")
    job_run_id = row_execucao.get("job_run_id")
    status_aws = row_execucao.get("status")

    resultado_base = {
        "job_run_id": job_run_id,
        "resultado_operacional": None,
        "status_operacional": None,
        "log_localizado": False,
        "qt_linhas_log": 0,
        "qt_erros_log": 0,
        "qt_warnings_log": 0,
        "possui_erro_log": False,
        "possui_warning_log": False,
        "erro_principal": None,
        "erro_principal_limpo": None,
        "objeto_erro": None,
        "timestamp_erro_principal": None,
        "detalhe_erro": None,
    }

    # EXPIRED e um estado final do Glue (nao esta em andamento); STOPPING esta em andamento
    status_falha = ["FAILED", "TIMEOUT", "TIMED_OUT", "STOPPED", "ERROR", "EXPIRED"]
    status_andamento = ["RUNNING", "STARTING", "WAITING", "STOPPING"]

    if status_aws == "SUCCEEDED":
        if avaliar_Logs_em_sucesso:
            df_logs = obter_logs_glue(job_name=job_name, job_run_id=job_run_id)
            classificacao_log = classificar_logs_glue(df_logs)
            resultado_base.update(classificacao_log)
            if resultado_base["possui_erro_log"]:
                resultado_base["resultado_operacional"] = "FALSO_POSITIVO"
                resultado_base["status_operacional"] = "SUCCEEDED_WITH_ERRORS"
                resultado_base["detalhe_erro"] = resultado_base["erro_principal_limpo"]
            elif resultado_base["possui_warning_log"]:
                resultado_base["resultado_operacional"] = "ALERTA"
                resultado_base["status_operacional"] = "SUCCEEDED_WITH_WARNINGS"
                resultado_base["detalhe_erro"] = resultado_base["erro_principal_limpo"]
            else:
                resultado_base["resultado_operacional"] = "POSITIVO"
                resultado_base["status_operacional"] = "SUCCEEDED"
        else:
            resultado_base["resultado_operacional"] = "POSITIVO"
            resultado_base["status_operacional"] = "SUCCEEDED"
    elif status_aws in status_falha:
        resultado_base["resultado_operacional"] = "FRACASSO"
        resultado_base["status_operacional"] = status_aws
        erro_aws = row_execucao.get("error_message")
        if not valor_vazio(erro_aws):
            resultado_base["detalhe_erro"] = limpar_mensagem_log(erro_aws)
        elif avaliar_Logs_em_falha:
            df_logs = obter_logs_glue(job_name=job_name, job_run_id=job_run_id)
            classificacao_log = classificar_logs_glue(df_logs)
            resultado_base.update(classificacao_log)
            resultado_base["detalhe_erro"] = resultado_base["erro_principal_limpo"]
    elif status_aws in status_andamento:
        resultado_base["resultado_operacional"] = "EM_EXECUCAO"
        resultado_base["status_operacional"] = status_aws
    else:
        resultado_base["resultado_operacional"] = "INDETERMINADO"
        resultado_base["status_operacional"] = status_aws

    return resultado_base


def avaliar_job_glue_por_payload(
    nome_job, data_referencia=None, filtro_input=None, timezone="America/Sao_Paulo",
    chaves_comparacao=None, avaliar_Logs_em_sucesso=True, avaliar_Logs_em_falha=True,
    ignorar_chaves_vazias_no_run=True, max_runs=None, verbose=True,
):
    """Avalia TODAS as execucoes de um Glue Job em um dia que correspondem a um filtro de payload.

    Classificacao (coluna "resultado_operacional"):
        POSITIVO        SUCCEEDED e sem erro/aviso no log (ou log nao avaliado).
        FALSO_POSITIVO  SUCCEEDED, mas o log contem mensagens de erro.
        ALERTA          SUCCEEDED, com avisos (WARN) no log.
        FRACASSO        FAILED, TIMEOUT, STOPPED, ERROR ou EXPIRED.
        EM_EXECUCAO     STARTING, RUNNING, WAITING ou STOPPING.
        INDETERMINADO   status desconhecido ou falha ao avaliar a execucao/logs.

    Parametros:
        nome_job (str): nome do Glue Job.
        data_referencia (None | date | datetime | str, opcional): dia a avaliar
            ("YYYY-MM-DD" ou "YYYYMMDD"). Padrao: hoje no fuso "timezone".
        filtro_input (None | dict | list[dict], opcional): payload(s) de filtro
            (ver filtrar_execucoes_por_payload). None avalia todas as execucoes.
        timezone (str, opcional): fuso usado para decidir o dia de cada execucao.
            Padrao: "America/Sao_Paulo".
        chaves_comparacao (list[str], opcional): compara apenas estas chaves do
            payload. Padrao: todas as chaves preenchidas do payload.
        avaliar_Logs_em_sucesso (bool, opcional): le o log das execucoes SUCCEEDED
            para detectar FALSO_POSITIVO/ALERTA. Padrao: True.
        avaliar_Logs_em_falha (bool, opcional): le o log das execucoes com falha
            quando a AWS nao informa ErrorMessage. Padrao: True.
        ignorar_chaves_vazias_no_run (bool, opcional): se True, uma chave do payload
            ausente (ou vazia) nos argumentos da execucao e ignorada, em vez de
            eliminar a execucao. Padrao: True.
        max_runs (int, opcional): para de buscar apos esse numero de execucoes do dia.
        verbose (bool, opcional): reservado; atualmente nao imprime nada.

    Retorna:
        dict com:
            df_execucoes: todas as execucoes do dia (argumentos expandidos);
            df_filtrado: as que casaram com o filtro;
            df_avaliacao: df_filtrado + colunas de avaliacao (avaliar_execucao_glue);
            df_problemas: linhas de df_avaliacao com FALSO_POSITIVO, FRACASSO,
                ALERTA ou INDETERMINADO;
            resumo: nome_job, data_referencia, total_execucoes_data,
                total_execucoes_filtradas e a contagem de cada classificacao.

    Exemplo:
        >>> r = avaliar_job_glue_por_payload("meu_job", "2026-09-27",
        ...                                  filtro_input={"tablename": "clientes"})
        >>> r["resumo"]["fracassos"]
    """
    data_ref = normalizar_data_referencia(data_referencia=data_referencia, timezone=timezone)
    df_execucoes = obter_execucoes_glue(nome_job=nome_job, data_referencia=data_ref, timezone=timezone, max_runs=max_runs)

    if df_execucoes.empty:
        resumo = {
            "nome_job": nome_job, "data_referencia": data_ref,
            "total_execucoes_data": 0, "total_execucoes_filtradas": 0,
            "positivos": 0, "falsos_positivos": 0, "fracassos": 0,
            "alertas": 0, "em_execucao": 0, "indeterminados": 0,
        }
        return {"df_execucoes": pd.DataFrame(), "df_filtrado": pd.DataFrame(), "df_avaliacao": pd.DataFrame(), "df_problemas": pd.DataFrame(), "resumo": resumo}

    df_execucoes = expandir_argumentos_glue(df_execucoes)
    df_filtrado = filtrar_execucoes_por_payload(df=df_execucoes, filtro_input=filtro_input, chaves_comparacao=chaves_comparacao, ignorar_chaves_vazias_no_run=ignorar_chaves_vazias_no_run, retornar_diagnostico_filtro=True)

    if df_filtrado.empty:
        resumo = {
            "nome_job": nome_job, "data_referencia": data_ref,
            "total_execucoes_data": len(df_execucoes), "total_execucoes_filtradas": 0,
            "positivos": 0, "falsos_positivos": 0, "fracassos": 0,
            "alertas": 0, "em_execucao": 0, "indeterminados": 0,
        }
        return {"df_execucoes": df_execucoes, "df_filtrado": pd.DataFrame(), "df_avaliacao": pd.DataFrame(), "df_problemas": pd.DataFrame(), "resumo": resumo}

    avaliacoes = []
    for _, row in df_filtrado.iterrows():
        try:
            avaliacao = avaliar_execucao_glue(row_execucao=row, avaliar_Logs_em_sucesso=avaliar_Logs_em_sucesso, avaliar_Logs_em_falha=avaliar_Logs_em_falha)
        except Exception as erro:
            avaliacao = {
                "job_run_id": row.get("job_run_id"), "status_aws": row.get("status"),
                "resultado_operacional": "INDETERMINADO", "status_operacional": row.get("status"),
                "log_localizado": False, "qt_linhas_log": 0, "qt_erros_log": 0, "qt_warnings_log": 0,
                "possui_erro_log": False, "possui_warning_log": False, "erro_principal": None,
                "erro_principal_limpo": None, "objeto_erro": None, "timestamp_erro_principal": None,
                "detalhe_erro": f"Erro ao avaliar execucao/logs: {erro}",
            }
        registro = row.to_dict()
        registro.update(avaliacao)
        avaliacoes.append(registro)

    df_avaliacao = pd.DataFrame(avaliacoes)
    if df_avaliacao.empty:
        df_problemas = pd.DataFrame()
    else:
        df_problemas = df_avaliacao[df_avaliacao["resultado_operacional"].isin(["FALSO_POSITIVO", "FRACASSO", "ALERTA", "INDETERMINADO"])].copy()

    resumo = {
        "nome_job": nome_job, "data_referencia": data_ref,
        "total_execucoes_data": len(df_execucoes), "total_execucoes_filtradas": len(df_filtrado),
        "positivos": int((df_avaliacao["resultado_operacional"] == "POSITIVO").sum()),
        "falsos_positivos": int((df_avaliacao["resultado_operacional"] == "FALSO_POSITIVO").sum()),
        "fracassos": int((df_avaliacao["resultado_operacional"] == "FRACASSO").sum()),
        "alertas": int((df_avaliacao["resultado_operacional"] == "ALERTA").sum()),
        "em_execucao": int((df_avaliacao["resultado_operacional"] == "EM_EXECUCAO").sum()),
        "indeterminados": int((df_avaliacao["resultado_operacional"] == "INDETERMINADO").sum()),
    }
    return {"df_execucoes": df_execucoes, "df_filtrado": df_filtrado, "df_avaliacao": df_avaliacao, "df_problemas": df_problemas, "resumo": resumo}


def avaliar_job_glue_por_lista_payloads_nomeados(
    nome_job, data_referencia=None, lista_payloads=None, last_execution=True,
    timezone="America/Sao_Paulo", chaves_comparacao=None,
    avaliar_Logs_em_sucesso=True, avaliar_Logs_em_falha=True,
    ignorar_chaves_vazias_no_run=True, max_runs=None,
    permitir_multiplos_matches=True, exigir_ao_menos_um_filtro_aplicado=True, verbose=True,
):
    """Avalia varias cargas nomeadas de um mesmo Glue Job em um dia (a funcao principal do modulo).

    Para cada (nome, payload) da lista, encontra as execucoes do dia cujos
    argumentos casam com o payload e classifica cada uma.

    Os payloads sao comparados com os argumentos de cada execucao, ja sem o
    prefixo "--" (ex.: {"tablename": "clientes"} casa com "--tablename clientes").
    Os valores sao comparados como texto normalizado (20260927.0 == "20260927").

    Classificacao (coluna "resultado_operacional"):
        POSITIVO        SUCCEEDED e sem erro/aviso no log (ou log nao avaliado).
        FALSO_POSITIVO  SUCCEEDED, mas o log contem mensagens de erro.
        ALERTA          SUCCEEDED, com avisos (WARN) no log.
        FRACASSO        FAILED, TIMEOUT, STOPPED, ERROR ou EXPIRED.
        EM_EXECUCAO     STARTING, RUNNING, WAITING ou STOPPING.
        INDETERMINADO   status desconhecido ou falha ao avaliar a execucao/logs.

    Parametros:
        nome_job (str): nome do Glue Job.
        data_referencia (None | date | datetime | str, opcional): dia a avaliar
            ("YYYY-MM-DD" ou "YYYYMMDD"). Padrao: hoje no fuso "timezone".
        lista_payloads (list[tuple[str, dict]] | None): [(nome, payload), ...].
            None avalia todas as execucoes com name="all".
        last_execution (bool, opcional): True avalia so a execucao mais recente de
            cada nome; False avalia todas. Padrao: True.
        timezone (str, opcional): fuso usado para decidir o dia de cada execucao.
            Padrao: "America/Sao_Paulo".
        chaves_comparacao (list[str], opcional): compara apenas estas chaves do
            payload. Padrao: todas as chaves preenchidas do payload.
        avaliar_Logs_em_sucesso (bool, opcional): le o log das execucoes SUCCEEDED
            para detectar FALSO_POSITIVO/ALERTA. Padrao: True.
        avaliar_Logs_em_falha (bool, opcional): le o log das execucoes com falha
            quando a AWS nao informa ErrorMessage. Padrao: True.
        ignorar_chaves_vazias_no_run (bool, opcional): se True, uma chave do payload
            ausente (ou vazia) nos argumentos da execucao e ignorada, em vez de
            eliminar a execucao. Padrao: True.
        max_runs (int, opcional): para de buscar apos esse numero de execucoes do dia.
        permitir_multiplos_matches (bool, opcional): se False, cada execucao conta
            so para o primeiro nome que ela atender. Padrao: True.
        exigir_ao_menos_um_filtro_aplicado (bool, opcional): descarta execucoes em
            que nenhuma chave do payload foi comparada. Padrao: True.
        verbose (bool, opcional): reservado; atualmente nao imprime nada.

    Retorna:
        dict com:
            df_execucoes: todas as execucoes do dia (argumentos expandidos);
            df_correlacionado: execucoes associadas a um nome (coluna "name");
            df_avaliacao: df_correlacionado + colunas de avaliacao;
            df_problemas: linhas com FALSO_POSITIVO, FRACASSO, ALERTA ou INDETERMINADO;
            df_resumo_name: contagem por nome e classificacao;
            resumo: ver montar_resumo_avaliacao_nomeada.
        Nomes sem nenhuma execucao no dia simplesmente nao aparecem nos DataFrames.

    Exemplo:
        >>> r = avaliar_job_glue_por_lista_payloads_nomeados(
        ...     "meu_job", "2026-09-27",
        ...     lista_payloads=[("Clientes", {"tablename": "clientes"}),
        ...                     ("Contratos", {"tablename": "contratos"})],
        ... )
        >>> r["df_resumo_name"]
    """
    data_ref = normalizar_data_referencia(data_referencia=data_referencia, timezone=timezone)
    df_execucoes = obter_execucoes_glue(nome_job=nome_job, data_referencia=data_ref, timezone=timezone, max_runs=max_runs)

    if df_execucoes is None or df_execucoes.empty:
        resumo = {
            "nome_job": nome_job, "data_referencia": data_ref, "last_execution": last_execution,
            "total_execucoes_data": 0, "total_execucoes_correlacionadas": 0, "total_execucoes_avaliadas": 0,
            "positivos": 0, "falsos_positivos": 0, "fracassos": 0, "alertas": 0, "em_execucao": 0, "indeterminados": 0,
        }
        return {"df_execucoes": pd.DataFrame(), "df_correlacionado": pd.DataFrame(), "df_avaliacao": pd.DataFrame(), "df_problemas": pd.DataFrame(), "df_resumo_name": pd.DataFrame(), "resumo": resumo}

    df_execucoes = expandir_argumentos_glue(df_execucoes)
    df_correlacionado = filtrar_execucoes_por_lista_payloads_nomeados(
        df_execucoes=df_execucoes, lista_payloads=lista_payloads, chaves_comparacao=chaves_comparacao,
        ignorar_chaves_vazias_no_run=ignorar_chaves_vazias_no_run, retornar_diagnostico_filtro=True,
        exigir_ao_menos_um_filtro_aplicado=exigir_ao_menos_um_filtro_aplicado,
        permitir_multiplos_matches=permitir_multiplos_matches,
    )

    if df_correlacionado is None or df_correlacionado.empty:
        resumo = {
            "nome_job": nome_job, "data_referencia": data_ref, "last_execution": last_execution,
            "total_execucoes_data": len(df_execucoes), "total_execucoes_correlacionadas": 0, "total_execucoes_avaliadas": 0,
            "positivos": 0, "falsos_positivos": 0, "fracassos": 0, "alertas": 0, "em_execucao": 0, "indeterminados": 0,
        }
        return {"df_execucoes": df_execucoes, "df_correlacionado": pd.DataFrame(), "df_avaliacao": pd.DataFrame(), "df_problemas": pd.DataFrame(), "df_resumo_name": pd.DataFrame(), "resumo": resumo}

    if last_execution:
        df_correlacionado = manter_ultima_execucao_por_name(df_correlacionado=df_correlacionado)
    else:
        df_correlacionado = df_correlacionado.sort_values(["name", "started_on"], ascending=[True, False]).reset_index(drop=True)
    df_correlacionado = reordenar_coluna_name_primeira(df_correlacionado)

    avaliacoes = []
    for _, row in df_correlacionado.iterrows():
        try:
            avaliacao = avaliar_execucao_glue(row_execucao=row, avaliar_Logs_em_sucesso=avaliar_Logs_em_sucesso, avaliar_Logs_em_falha=avaliar_Logs_em_falha)
        except Exception as erro:
            avaliacao = {
                "job_run_id": row.get("job_run_id"), "status_aws": row.get("status"),
                "resultado_operacional": "INDETERMINADO", "status_operacional": row.get("status"),
                "log_localizado": False, "qt_linhas_log": 0, "qt_erros_log": 0, "qt_warnings_log": 0,
                "possui_erro_log": False, "possui_warning_log": False, "erro_principal": None,
                "erro_principal_limpo": None, "objeto_erro": None, "timestamp_erro_principal": None,
                "detalhe_erro": f"Erro ao avaliar execucao/logs: {erro}",
            }
        registro = row.to_dict()
        registro.update(avaliacao)
        avaliacoes.append(registro)

    df_avaliacao = pd.DataFrame(avaliacoes)
    df_avaliacao = reordenar_coluna_name_primeira(df_avaliacao)
    if df_avaliacao.empty:
        df_problemas = pd.DataFrame()
    else:
        df_problemas = df_avaliacao[df_avaliacao["resultado_operacional"].isin(["FALSO_POSITIVO", "FRACASSO", "ALERTA", "INDETERMINADO"])].copy()
        df_problemas = reordenar_coluna_name_primeira(df_problemas)

    resumo, df_resumo_name = montar_resumo_avaliacao_nomeada(
        nome_job=nome_job, data_referencia=data_ref, df_execucoes=df_execucoes,
        df_correlacionado=df_correlacionado, df_avaliacao=df_avaliacao, last_execution=last_execution,
    )
    return {"df_execucoes": df_execucoes, "df_correlacionado": df_correlacionado, "df_avaliacao": df_avaliacao, "df_problemas": df_problemas, "df_resumo_name": df_resumo_name, "resumo": resumo}


# ============================================================
# 13. GLUE - LOGS E CLASSIFICACAO DE ERRO
# ============================================================

def obter_logs_glue(job_name, job_run_id, Log_group_base=None, sufixos_Log_group=("output",), incluir_ingestion_time=True):
    """Le os eventos de log de uma execucao do Glue no CloudWatch Logs.

    O stream lido tem o nome do job_run_id, dentro de "<Log_group_base>/<sufixo>"
    para cada sufixo informado (no Glue: "output" = saida do driver, "error" =
    stderr).

    Parametros:
        job_name (str): nome do Glue Job.
        job_run_id (str): id da execucao (ex.: "jr_abc123...").
        Log_group_base (str, opcional): grupo base. Padrao: o LogGroupName da execucao
            (normalmente "/aws-glue/jobs").
        sufixos_Log_group (tuple[str], opcional): sufixos lidos. Padrao: ("output",).
            Use ("output", "error") para incluir o stderr.
        incluir_ingestion_time (bool, opcional): inclui a coluna ingestion_time. Padrao: True.

    Retorna:
        pandas.DataFrame: colunas timestamp, mensagem, log_group, log_stream (e
        ingestion_time), ordenado por timestamp. Se nenhum grupo puder ser lido,
        uma linha com a coluna erro_obter_log descrevendo as falhas.
    """
    glue = boto3.client("glue")
    logs = boto3.client("logs")

    if valor_vazio(Log_group_base):
        job_run = glue.get_job_run(JobName=job_name, RunId=job_run_id)["JobRun"]
        Log_group_base = job_run.get("LogGroupName")
        if valor_vazio(Log_group_base):
            return pd.DataFrame(columns=["timestamp", "mensagem", "log_group", "log_stream", "erro_obter_log"])

    eventos = []
    erros_log_group = []

    for sufixo in sufixos_Log_group:
        if str(Log_group_base).endswith(f"/{sufixo}"):
            log_group = Log_group_base
        else:
            log_group = f"{Log_group_base}/{sufixo}"

        next_token = None
        try:
            while True:
                parametros = {"logGroupName": log_group, "logStreamName": job_run_id, "startFromHead": True}
                if next_token:
                    parametros["nextToken"] = next_token
                response = logs.get_log_events(**parametros)
                for evento in response.get("events", []):
                    registro = {
                        # CloudWatch devolve timestamp em milissegundos (com "ns" as datas iam para 1970)
                        "timestamp": pd.to_datetime(evento.get("timestamp"), unit="ms"),
                        "mensagem": evento.get("message"),
                        "log_group": log_group,
                        "log_stream": job_run_id,
                    }
                    if incluir_ingestion_time:
                        registro["ingestion_time"] = pd.to_datetime(evento.get("ingestionTime"), unit="ms")
                    eventos.append(registro)
                novo_token = response.get("nextForwardToken")
                if novo_token == next_token:
                    break
                next_token = novo_token
        except Exception as erro:
            erros_log_group.append({"log_group": log_group, "erro": str(erro)})
            continue

    if eventos:
        return pd.DataFrame(eventos).sort_values("timestamp").reset_index(drop=True)
    if erros_log_group:
        return pd.DataFrame([{"timestamp": None, "mensagem": None, "log_group": None, "log_stream": job_run_id, "erro_obter_log": erros_log_group}])
    return pd.DataFrame(columns=["timestamp", "mensagem", "log_group", "log_stream", "erro_obter_log"])


def limpar_mensagem_log(mensagem):
    """Remove quebras de linha e espacos repetidos de uma mensagem de log.

    Parametros:
        mensagem (Any): mensagem original.

    Retorna:
        str | None: texto em uma unica linha (None se a mensagem for None).
    """
    if mensagem is None:
        return None
    texto = str(mensagem)
    texto = texto.replace("\n", " ")
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto


def extrair_objeto_erro(mensagem):
    """Tenta extrair o nome da tabela citada em uma mensagem de erro ("tabela X" / "na tabela X").

    Parametros:
        mensagem (str): mensagem de erro.

    Retorna:
        str | None: nome encontrado (ex.: "db.minha_tabela") ou None.
    """
    if not mensagem:
        return None
    texto = limpar_mensagem_log(mensagem)
    padroes = [r"na tabela\s+([a-zA-Z0-9_\.]+)", r"tabela\s+([a-zA-Z0-9_\.]+)"]
    for padrao in padroes:
        match = re.search(padrao, texto, flags=re.IGNORECASE)
        if match:
            if len(match.groups()) == 2:
                return f"{match.group(1)}.{match.group(2)}"
            return match.group(1)
    return None


def classificar_logs_glue(df_Logs):
    """Procura erros e avisos nas mensagens de log de uma execucao.

    E uma heuristica por expressao regular (sem diferenciar maiusculas):
        erro:  "-SEVERE", "-ERROR", a palavra ERROR, AnalysisException,
               PySparkError, "Exception:", ValueError, KeyError e mensagens de
               quantidade de colunas divergente em INSERT;
        aviso: "-WARN", WARN, WARNING.
    Mensagens benignas que contenham essas palavras tambem sao contadas.

    Parametros:
        df_Logs (pandas.DataFrame): resultado de obter_logs_glue (coluna "mensagem").

    Retorna:
        dict com: log_localizado, qt_linhas_log, qt_erros_log, qt_warnings_log,
        possui_erro_log, possui_warning_log, erro_principal (primeira mensagem de
        erro), erro_principal_limpo, objeto_erro, timestamp_erro_principal e
        status_log ("SEM_LOG", "LOG_INVALIDO", "COM_ERRO", "COM_WARNING" ou "SEM_ERRO").
    """
    if df_Logs is None or df_Logs.empty:
        return {"log_localizado": False, "qt_linhas_log": 0, "qt_erros_log": 0, "qt_warnings_log": 0, "possui_erro_log": False, "possui_warning_log": False, "erro_principal": None, "erro_principal_limpo": None, "objeto_erro": None, "timestamp_erro_principal": None, "status_log": "SEM_LOG"}

    if "mensagem" not in df_Logs.columns:
        return {"log_localizado": False, "qt_linhas_log": len(df_Logs), "qt_erros_log": 0, "qt_warnings_log": 0, "possui_erro_log": False, "possui_warning_log": False, "erro_principal": None, "erro_principal_limpo": None, "objeto_erro": None, "timestamp_erro_principal": None, "status_log": "LOG_INVALIDO"}

    mensagens = df_Logs["mensagem"].fillna("").astype(str)
    padrao_erro = r"\s-SEVERE|\s-ERROR|\bERROR\b|AnalysisException|PySparkError|Exception:|ValueError|KeyError|requires that the data to be inserted|target table has|inserted data has"
    padrao_warning = r"\s-WARN|\bWARN\b|\bWARNING\b"

    mascara_erros = mensagens.str.contains(padrao_erro, case=False, na=False, regex=True)
    mascara_warnings = mensagens.str.contains(padrao_warning, case=False, na=False, regex=True)
    df_erros = df_Logs.loc[mascara_erros].copy()
    df_warnings = df_Logs.loc[mascara_warnings].copy()

    erro_principal = None
    timestamp_erro_principal = None
    if not df_erros.empty:
        erro_principal = df_erros.iloc[0].get("mensagem")
        timestamp_erro_principal = df_erros.iloc[0].get("timestamp")
        erro_principal_limpo = limpar_mensagem_log(erro_principal)
        objeto_erro = extrair_objeto_erro(erro_principal_limpo)
    else:
        erro_principal_limpo = None
        objeto_erro = None

    return {
        "log_localizado": True,
        "qt_linhas_log": len(df_Logs),
        "qt_erros_log": len(df_erros),
        "qt_warnings_log": len(df_warnings),
        "possui_erro_log": len(df_erros) > 0,
        "possui_warning_log": len(df_warnings) > 0,
        "erro_principal": erro_principal,
        "erro_principal_limpo": erro_principal_limpo,
        "objeto_erro": objeto_erro,
        "timestamp_erro_principal": timestamp_erro_principal,
        "status_log": "COM_ERRO" if len(df_erros) > 0 else ("COM_WARNING" if len(df_warnings) > 0 else "SEM_ERRO"),
    }


def classificar_erro_detalhado(mensagem: Any) -> Dict[str, Optional[str]]:
    """Classifica uma mensagem de erro em tres niveis: categoria do Glue, framework e tipo de erro.

    Parametros:
        mensagem (Any): texto do erro (ex.: error_message da execucao).

    Retorna:
        dict com:
            categoria_glue (str | None): valor de "Error Category: X", se houver;
            erro_framework (str | None): AnalysisException, ParseException,
                AmazonS3Exception, ExecutorLostFailure, HiveMetastore, Iceberg,
                Athena, Spark, Oracle ou JDBC;
            tipo_erro (str): COLUNA_INEXISTENTE, TABELA_NAO_ENCONTRADA,
                ARQUIVO_NAO_ENCONTRADO, PERMISSAO, MEMORIA, FALHA_EXECUTOR,
                RECURSO_INDISPONIVEL, TIMEOUT, ERRO_QUERY, ERRO_SINTAXE,
                ERRO_METASTORE, ERRO_ICEBERG, ERRO_S3 ou OUTROS.

    Exemplo:
        >>> classificar_erro_detalhado("AnalysisException: Table or view not found: db.x")
        {'categoria_glue': None, 'erro_framework': 'AnalysisException', 'tipo_erro': 'TABELA_NAO_ENCONTRADA'}
    """
    mensagem_original = str(mensagem)
    msg = mensagem_original.upper()
    categoria_glue = None
    erro_framework = None
    tipo_erro = "OUTROS"

    categoria_match = re.search(r"Error Category:\s*([A-Z]+)", mensagem_original, flags=re.IGNORECASE)
    if categoria_match:
        categoria_glue = categoria_match.group(1).strip()

    if "ANALYSISEXCEPTION" in msg:
        erro_framework = "AnalysisException"
    elif "PARSEEXCEPTION" in msg:
        erro_framework = "ParseException"
    elif "AMAZONS3EXCEPTION" in msg:
        erro_framework = "AmazonS3Exception"
    elif "EXECUTORLOSTFAILURE" in msg:
        erro_framework = "ExecutorLostFailure"
    elif "HIVE_METASTORE" in msg:
        erro_framework = "HiveMetastore"
    elif "ICEBERG" in msg:
        erro_framework = "Iceberg"
    elif "ATHENA" in msg:
        erro_framework = "Athena"
    elif "SPARK" in msg:
        erro_framework = "Spark"
    elif "ORA-" in msg:
        erro_framework = "Oracle"
    elif "JDBC" in msg:
        erro_framework = "JDBC"

    if "CANNOT RESOLVE" in msg:
        tipo_erro = "COLUNA_INEXISTENTE"
    elif "TABLE OR VIEW NOT FOUND" in msg:
        tipo_erro = "TABELA_NAO_ENCONTRADA"
    elif "PATH DOES NOT EXIST" in msg or "NO SUCH FILE" in msg or "FILE NOT FOUND" in msg:
        tipo_erro = "ARQUIVO_NAO_ENCONTRADO"
    elif "ACCESS DENIED" in msg or "PERMISSION" in msg:
        tipo_erro = "PERMISSAO"
    elif "OUT OF MEMORY" in msg or "MEMORY" in msg:
        tipo_erro = "MEMORIA"
    elif "EXECUTORLOSTFAILURE" in msg:
        tipo_erro = "FALHA_EXECUTOR"
    elif "RESOURCE_UNAVAILABLE" in msg:
        tipo_erro = "RECURSO_INDISPONIVEL"
    elif "TIMEOUT" in msg or "TIMED OUT" in msg:
        tipo_erro = "TIMEOUT"
    elif "QUERY_ERROR" in msg:
        tipo_erro = "ERRO_QUERY"
    elif "SYNTAX" in msg:
        tipo_erro = "ERRO_SINTAXE"
    elif "HIVE_METASTORE" in msg:
        tipo_erro = "ERRO_METASTORE"
    elif "ICEBERG" in msg:
        tipo_erro = "ERRO_ICEBERG"
    elif "AMAZONS3EXCEPTION" in msg:
        tipo_erro = "ERRO_S3"

    return {"categoria_glue": categoria_glue, "erro_framework": erro_framework, "tipo_erro": tipo_erro}


def classificar_erro_glue_visual(detalhe_erro, objeto_erro=None, dicionario_erros=None):
    """Traduz um erro tecnico em campos legiveis usando um dicionario de erros conhecidos.

    A primeira chave de "dicionario_erros" contida no texto do erro (sem
    diferenciar maiusculas) define o resultado. Sem correspondencia, o erro fica
    como "NAO_CLASSIFICADO".

    Parametros:
        detalhe_erro (str | None): texto do erro. Vazio devolve todos os campos None.
        objeto_erro (str, opcional): tabela/objeto relacionado ao erro.
        dicionario_erros (dict, opcional): {trecho_da_mensagem: {"erro_categoria": ...,
            "erro_resumido": ..., "impacto_negocio": ..., "acao_sugerida": ...}}.

    Retorna:
        dict com: erro_categoria, erro_resumido, impacto_negocio, acao_sugerida e objeto_erro_visual.

    Exemplo:
        >>> erros = {"Access Denied": {"erro_categoria": "PERMISSAO",
        ...          "erro_resumido": "Sem acesso ao S3", "impacto_negocio": "Carga nao atualizada",
        ...          "acao_sugerida": "Revisar a role do job"}}
        >>> classificar_erro_glue_visual("... Access Denied ...", dicionario_erros=erros)["erro_categoria"]
        'PERMISSAO'
    """
    if dicionario_erros is None:
        dicionario_erros = {}
    retorno = {"erro_categoria": None, "erro_resumido": None, "impacto_negocio": None, "acao_sugerida": None, "objeto_erro_visual": objeto_erro}
    if valor_vazio(detalhe_erro):
        return retorno
    texto = str(detalhe_erro)
    for chave, tratamento in dicionario_erros.items():
        if chave.lower() in texto.lower():
            retorno.update(tratamento)
            break
    if retorno["erro_categoria"] is None:
        retorno.update({"erro_categoria": "NAO_CLASSIFICADO", "erro_resumido": "Erro nao classificado.", "impacto_negocio": "Necessaria avaliacao tecnica.", "acao_sugerida": "Validar detalhe tecnico."})
    return retorno


def gerar_diagnostico(info: Dict[str, Any]) -> Optional[str]:
    """Gera um texto de diagnostico a partir de um erro ja interpretado.

    Parametros:
        info (dict): chaves usadas (todas opcionais): tipo_erro (ver
            classificar_erro_detalhado), linha_erro, job_name, state_name,
            coluna_nao_resolvida, colunas_disponiveis_limpas, colunas_com_espaco,
            categoria_glue e erro_framework.

    Retorna:
        str | None: texto explicativo com a acao sugerida, ou None se tipo_erro nao
        tiver diagnostico (ex.: "OUTROS").

    Exemplo:
        >>> info = classificar_erro_detalhado(msg)
        >>> info.update(job_name="meu_job", coluna_nao_resolvida=extrair_coluna_nao_resolvida(msg))
        >>> print(gerar_diagnostico(info))
    """
    tipo_erro = info.get("tipo_erro")
    linha_erro = info.get("linha_erro")
    job_name = info.get("job_name")
    state_name = info.get("state_name")
    coluna = info.get("coluna_nao_resolvida")
    colunas_limpas = info.get("colunas_disponiveis_limpas")
    colunas_espaco = info.get("colunas_com_espaco")
    categoria_glue = info.get("categoria_glue")
    erro_framework = info.get("erro_framework")

    contexto = ""
    if state_name:
        contexto += f"State da Step Function: {state_name}. "
    if categoria_glue:
        contexto += f"Categoria Glue: {categoria_glue}. "
    if erro_framework:
        contexto += f"Framework/erro tecnico: {erro_framework}. "

    if tipo_erro == "COLUNA_INEXISTENTE":
        diagnostico = f"{contexto}Coluna nao encontrada no processamento. Verificar a linha {linha_erro} do job {job_name}. "
        if coluna:
            diagnostico += f"A coluna referenciada foi '{coluna}'. "
        if coluna and colunas_limpas and coluna in colunas_limpas:
            diagnostico += f"A coluna '{coluna}' aparece na lista de colunas disponiveis apos remover espacos, indicando provavel problema de espacos no nome da coluna. "
        if colunas_espaco:
            diagnostico += f"Foram detectadas colunas com espacos suspeitos no nome: {colunas_espaco}. Considere aplicar strip nas colunas antes de selecionar, renomear ou usar em query."
        return diagnostico
    if tipo_erro == "TABELA_NAO_ENCONTRADA":
        return f"{contexto}Tabela ou view nao encontrada. Verificar referencia de tabela no job {job_name}, proximo da linha {linha_erro}."
    if tipo_erro == "ARQUIVO_NAO_ENCONTRADO":
        return f"{contexto}Arquivo ou caminho nao encontrado. Verificar paths de entrada do job {job_name}."
    if tipo_erro == "PERMISSAO":
        return f"{contexto}Erro de permissao. Verificar role, bucket, prefixo S3 ou permissoes associadas ao job {job_name}."
    if tipo_erro == "MEMORIA":
        return f"{contexto}Possivel erro de memoria no Glue. Verificar volume de dados, particionamento, worker type e numero de workers."
    if tipo_erro == "TIMEOUT":
        return f"{contexto}Execucao finalizou por timeout. Verificar timeout, volume de dados e performance do processamento."
    if tipo_erro == "RECURSO_INDISPONIVEL":
        return f"{contexto}Recurso indisponivel. Verificar capacidade, workers, Glue Flex ou limites da conta."
    if tipo_erro == "FALHA_EXECUTOR":
        return f"{contexto}Falha de executor Spark. Verificar logs do Glue, volume de dados, skew, memoria e numero de workers."
    return None


def enriquecer_df_avaliacao_glue(df_avaliacao, dicionario_erros=None):
    """Adiciona a um DataFrame de avaliacao colunas amigaveis sobre o erro de cada execucao.

    Colunas adicionadas: erro_categoria, erro_resumido, impacto_negocio,
    acao_sugerida (via classificar_erro_glue_visual), coluna_problematica,
    colunas_tabela, colunas_carga e diferenca_colunas.

    Parametros:
        df_avaliacao (pandas.DataFrame): df_avaliacao das funcoes avaliar_*
            (usa as colunas detalhe_erro e objeto_erro).
        dicionario_erros (dict, opcional): ver classificar_erro_glue_visual.

    Retorna:
        pandas.DataFrame: novo DataFrame (vazio se a entrada for vazia).
    """
    if df_avaliacao is None or df_avaliacao.empty:
        return pd.DataFrame()
    if dicionario_erros is None:
        dicionario_erros = {}

    registros_visuais = []
    for _, row in df_avaliacao.iterrows():
        detalhe_erro = row.get("detalhe_erro")
        objeto_erro = row.get("objeto_erro")
        erro_visual = classificar_erro_glue_visual(detalhe_erro=detalhe_erro, objeto_erro=objeto_erro, dicionario_erros=dicionario_erros)
        coluna_problematica = extrair_coluna_problematica(detalhe_erro)
        info_colunas = extrair_diferenca_colunas(detalhe_erro)
        registro = {
            "erro_categoria": erro_visual.get("erro_categoria"),
            "erro_resumido": erro_visual.get("erro_resumido"),
            "impacto_negocio": erro_visual.get("impacto_negocio"),
            "acao_sugerida": erro_visual.get("acao_sugerida"),
            "coluna_problematica": coluna_problematica,
            "colunas_tabela": info_colunas.get("colunas_tabela"),
            "colunas_carga": info_colunas.get("colunas_carga"),
            "diferenca_colunas": info_colunas.get("diferenca_colunas"),
        }
        registros_visuais.append(registro)
    df_visual = pd.concat([df_avaliacao.reset_index(drop=True), pd.DataFrame(registros_visuais)], axis=1)
    return df_visual


def extrair_coluna_nao_resolvida(mensagem: Any) -> Optional[str]:
    """Extrai o nome da coluna de mensagens Spark do tipo "cannot resolve 'coluna'".

    Parametros:
        mensagem (Any): mensagem de erro.

    Retorna:
        str | None
    """
    mensagem = str(mensagem)
    match = re.search(r"cannot resolve\s+['\"]?([a-zA-Z0-9_]+)['\"]?", mensagem, flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def extrair_colunas_disponiveis(mensagem: Any) -> Optional[List[str]]:
    """Extrai a lista de "given input columns: [...]" de uma mensagem Spark, sem limpar espacos.

    Parametros:
        mensagem (Any): mensagem de erro.

    Retorna:
        list[str] | None: nomes como aparecem na mensagem (use limpar_colunas_disponiveis).
    """
    mensagem = str(mensagem)
    match = re.search(r"given input columns:\s*\[([^\]]+)\]", mensagem, flags=re.IGNORECASE)
    if not match:
        return None
    return [col for col in match.group(1).split(",")]


def limpar_colunas_disponiveis(colunas: Optional[List[str]]) -> Optional[List[str]]:
    """Remove espacos no inicio e no fim de cada nome de coluna.

    Parametros:
        colunas (list[str] | None): nomes de colunas.

    Retorna:
        list[str] | None: None se a lista for vazia ou None.
    """
    if not colunas:
        return None
    return [str(col).strip() for col in colunas]


def detectar_colunas_com_espaco_suspeito(colunas: Optional[List[str]]) -> List[Dict[str, Any]]:
    """Aponta colunas cujo nome tem espacos extras (causa comum de "cannot resolve").

    O primeiro espaco apos a virgula da lista (separador) e desconsiderado.

    Parametros:
        colunas (list[str] | None): nomes como vieram de extrair_colunas_disponiveis.

    Retorna:
        list[dict]: [{"posicao": int, "coluna_original": str, "coluna_limpa": str}, ...].
    """
    if not colunas:
        return []
    suspeitas = []
    for pos, col_original in enumerate(colunas):
        col_original = str(col_original)
        if pos > 0 and col_original.startswith(" "):
            col_sem_separador = col_original[1:]
        else:
            col_sem_separador = col_original
        col_limpa = col_original.strip()
        if col_sem_separador != col_sem_separador.strip():
            suspeitas.append({"posicao": pos, "coluna_original": col_original, "coluna_limpa": col_limpa})
    return suspeitas


def extrair_coluna_problematica(detalhe_erro):
    """Extrai a coluna citada em erros de cast ou de "column X".

    Parametros:
        detalhe_erro (str | None): texto do erro.

    Retorna:
        str | None
    """
    if valor_vazio(detalhe_erro):
        return None
    padroes = [r"Cannot safely cast '([a-zA-Z0-9_]+)'", r"column ['\"]?([a-zA-Z0-9_]+)", r"column ([a-zA-Z0-9_]+)"]
    for padrao in padroes:
        match = re.search(padrao, str(detalhe_erro), flags=re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def extrair_diferenca_colunas(detalhe_erro):
    """Extrai a quantidade de colunas de erros "target table has N ... inserted data has M".

    Parametros:
        detalhe_erro (str | None): texto do erro.

    Retorna:
        dict: {"colunas_tabela": N, "colunas_carga": M, "diferenca_colunas": N - M}
        (valores None se o padrao nao for encontrado).
    """
    resultado = {"colunas_tabela": None, "colunas_carga": None, "diferenca_colunas": None}
    if valor_vazio(detalhe_erro):
        return resultado
    texto = str(detalhe_erro)
    match = re.search(r"target table has\s+(\d+).*inserted data has\s+(\d+)", texto, flags=re.IGNORECASE)
    if not match:
        return resultado
    tabela = int(match.group(1))
    carga = int(match.group(2))
    resultado.update({"colunas_tabela": tabela, "colunas_carga": carga, "diferenca_colunas": tabela - carga})
    return resultado


def avaliar_ultima_execucao_glue_por_payload(
    nome_job,
    filtro_input=None,
    chaves_comparacao=None,
    ignorar_chaves_vazias_no_run=True,
    data_referencia=None,
    timezone="America/Sao_Paulo",
    max_runs=None,
    verbose=True,
):
    """Avalia apenas a execucao MAIS RECENTE do dia que corresponde a um filtro de payload.

    Os logs sao sempre lidos (sucesso e falha). O resultado ja vem enriquecido
    com as colunas de enriquecer_df_avaliacao_glue.

    Classificacao (coluna "resultado_operacional"):
        POSITIVO        SUCCEEDED e sem erro/aviso no log (ou log nao avaliado).
        FALSO_POSITIVO  SUCCEEDED, mas o log contem mensagens de erro.
        ALERTA          SUCCEEDED, com avisos (WARN) no log.
        FRACASSO        FAILED, TIMEOUT, STOPPED, ERROR ou EXPIRED.
        EM_EXECUCAO     STARTING, RUNNING, WAITING ou STOPPING.
        INDETERMINADO   status desconhecido ou falha ao avaliar a execucao/logs.

    Parametros:
        nome_job (str): nome do Glue Job.
        filtro_input (None | dict | list[dict], opcional): payload(s) de filtro.
        chaves_comparacao (list[str], opcional): compara apenas estas chaves do
            payload. Padrao: todas as chaves preenchidas do payload.
        ignorar_chaves_vazias_no_run (bool, opcional): se True, uma chave do payload
            ausente (ou vazia) nos argumentos da execucao e ignorada, em vez de
            eliminar a execucao. Padrao: True.
        data_referencia (None | date | datetime | str, opcional): dia a avaliar
            ("YYYY-MM-DD" ou "YYYYMMDD"). Padrao: hoje.
        timezone (str, opcional): fuso usado para decidir o dia de cada execucao.
            Padrao: "America/Sao_Paulo".
        max_runs (int, opcional): para de buscar apos esse numero de execucoes do dia.
        verbose (bool, opcional): imprime o resumo e os problemas. Padrao: True.

    Retorna:
        dict com: df_execucoes, df_filtrado, df_ultima_execucao, df_avaliacao,
        df_problemas e resumo (nome_job, data_referencia, total_execucoes_data,
        total_execucoes_filtradas, avaliou_ultima, resultado_operacional,
        status_operacional, job_run_id, detalhe_erro).

    Exemplo:
        >>> r = avaliar_ultima_execucao_glue_por_payload("meu_job", {"tablename": "clientes"})
        >>> r["resumo"]["resultado_operacional"]
    """
    import pandas as pd

    data_ref = normalizar_data_referencia(
        data_referencia=data_referencia,
        timezone=timezone,
    )

    df_execucoes = obter_execucoes_glue(
        nome_job=nome_job,
        data_referencia=data_ref,
        timezone=timezone,
        max_runs=max_runs,
    )

    if df_execucoes.empty:
        resumo = {
            "nome_job": nome_job,
            "data_referencia": data_ref,
            "total_execucoes_data": 0,
            "total_execucoes_filtradas": 0,
            "avaliou_ultima": False,
            "resultado_operacional": None,
            "status_operacional": None,
            "job_run_id": None,
            "detalhe_erro": None,
        }
        if verbose:
            print("=" * 100)
            print("AVALIACAO DA ULTIMA EXECUCAO DO GLUE JOB")
            print("=" * 100)
            print(f"Job.........................: {nome_job}")
            print(f"Data referncia.............: {data_ref}")
            print("Nenhuma execucao encontrada na data.")
            print("=" * 100)

        return {
            "df_execucoes": pd.DataFrame(),
            "df_filtrado": pd.DataFrame(),
            "df_ultima_execucao": pd.DataFrame(),
            "df_avaliacao": pd.DataFrame(),
            "df_problemas": pd.DataFrame(),
            "resumo": resumo,
        }

    df_execucoes = expandir_argumentos_glue(df_execucoes)

    df_filtrado = filtrar_execucoes_por_payload(
        df=df_execucoes,
        filtro_input=filtro_input,
        chaves_comparacao=chaves_comparacao,
        ignorar_chaves_vazias_no_run=ignorar_chaves_vazias_no_run,
        retornar_diagnostico_filtro=True,
    )

    if df_filtrado.empty:
        resumo = {
            "nome_job": nome_job,
            "data_referencia": data_ref,
            "total_execucoes_data": len(df_execucoes),
            "total_execucoes_filtradas": 0,
            "avaliou_ultima": False,
            "resultado_operacional": None,
            "status_operacional": None,
            "job_run_id": None,
            "detalhe_erro": None,
        }
        if verbose:
            print("=" * 100)
            print("AVALIACAO DA ULTIMA EXECUCAO DO GLUE JOB")
            print("=" * 100)
            print(f"Job.........................: {nome_job}")
            print(f"Data referncia.............: {data_ref}")
            print(f"Total execucoes na data.....: {len(df_execucoes)}")
            print("Total apos filtro...........: 0")
            print("Nenhuma execucao restou apos o filtro.")
            print("=" * 100)

        return {
            "df_execucoes": df_execucoes,
            "df_filtrado": pd.DataFrame(),
            "df_ultima_execucao": pd.DataFrame(),
            "df_avaliacao": pd.DataFrame(),
            "df_problemas": pd.DataFrame(),
            "resumo": resumo,
        }

    df_filtrado = df_filtrado.sort_values("started_on", ascending=False)
    df_ultima_execucao = df_filtrado.head(1)

    df_avaliacao = df_ultima_execucao.copy()
    df_avaliacao["resultado_operacional"] = None
    df_avaliacao["status_operacional"] = None
    df_avaliacao["detalhe_erro"] = None

    for idx, row in df_avaliacao.iterrows():
        resultado = avaliar_execucao_glue(row)
        df_avaliacao.at[idx, "resultado_operacional"] = resultado.get("resultado_operacional")
        df_avaliacao.at[idx, "status_operacional"] = resultado.get("status_operacional")
        df_avaliacao.at[idx, "detalhe_erro"] = resultado.get("detalhe_erro")

    df_avaliacao = enriquecer_df_avaliacao_glue(df_avaliacao)

    df_problemas = df_avaliacao[
        df_avaliacao["resultado_operacional"].isin(["FRACASSO", "ALERTA", "INDETERMINADO", "FALSO_POSITIVO"])
    ]

    resumo = {
        "nome_job": nome_job,
        "data_referencia": data_ref,
        "total_execucoes_data": len(df_execucoes),
        "total_execucoes_filtradas": len(df_filtrado),
        "avaliou_ultima": True,
        "resultado_operacional": df_avaliacao.iloc[0]["resultado_operacional"] if not df_avaliacao.empty else None,
        "status_operacional": df_avaliacao.iloc[0]["status_operacional"] if not df_avaliacao.empty else None,
        "job_run_id": df_avaliacao.iloc[0]["job_run_id"] if not df_avaliacao.empty else None,
        "detalhe_erro": df_avaliacao.iloc[0]["detalhe_erro"] if not df_avaliacao.empty else None,
    }

    if verbose:
        print("=" * 100)
        print("AVALIACAO DA ULTIMA EXECUCAO DO GLUE JOB")
        print("=" * 100)
        print(f"Job.........................: {nome_job}")
        print(f"Data referncia.............: {data_ref}")
        print(f"Total execucoes na data.....: {len(df_execucoes)}")
        print(f"Total apos filtro...........: {len(df_filtrado)}")
        print(f"Resultado operacional.......: {resumo['resultado_operacional']}")
        print(f"Status operacional..........: {resumo['status_operacional']}")
        print(f"Job Run ID..................: {resumo['job_run_id']}")
        if resumo['detalhe_erro']:
            print(f"Detalhe erro................: {resumo['detalhe_erro']}")
        print("=" * 100)

        if not df_problemas.empty:
            print("\nProblemas detectados:")
            colunas_mostrar = [
                "job_run_id", "started_on", "completed_on", "resultado_operacional",
                "status_operacional", "detalhe_erro",
            ]
            colunas_mostrar = [col for col in colunas_mostrar if col in df_problemas.columns]
            try:
                print_df(df_problemas[colunas_mostrar])
            except ImportError:  # tabulate nao instalado
                print(df_problemas[colunas_mostrar])

    return {
        "df_execucoes": df_execucoes,
        "df_filtrado": df_filtrado,
        "df_ultima_execucao": df_ultima_execucao,
        "df_avaliacao": df_avaliacao,
        "df_problemas": df_problemas,
        "resumo": resumo,
    }


def avaliar_job_glue_visual(*args, dicionario_erros=None, **kwargs):
    """Executa avaliar_job_glue_por_lista_payloads_nomeados e acrescenta colunas amigaveis de erro.

    Aceita exatamente os mesmos parametros de avaliar_job_glue_por_lista_payloads_nomeados,
    mais:
        dicionario_erros (dict, opcional): ver classificar_erro_glue_visual.

    Retorna:
        dict: o mesmo de avaliar_job_glue_por_lista_payloads_nomeados, mais:
            df_avaliacao_visual: df_avaliacao enriquecido (enriquecer_df_avaliacao_glue);
            df_problemas_visual: linhas com FALSO_POSITIVO, FRACASSO, ALERTA ou INDETERMINADO.
    """
    resultado = avaliar_job_glue_por_lista_payloads_nomeados(
        *args,
        **kwargs,
    )

    df_visual = enriquecer_df_avaliacao_glue(
        resultado["df_avaliacao"],
        dicionario_erros=dicionario_erros,
    )

    resultado["df_avaliacao_visual"] = df_visual
    if df_visual.empty or "resultado_operacional" not in df_visual.columns:
        # sem execucoes avaliadas
        resultado["df_problemas_visual"] = pd.DataFrame()
    else:
        resultado["df_problemas_visual"] = df_visual[
            df_visual["resultado_operacional"].isin(STATUS_PROBLEMA)
        ].copy()

    return resultado
