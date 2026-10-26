"""datas - Calculo de datas de referencia para processamentos diarios, mensais e trimestrais.

- avaliar_dia / avaliar_dia_hoje: dias validos anteriores, com regra de fechamento de periodo.
- avaliar_dia_diario: dias uteis (seg-sex) anteriores a hoje.
- avaliar_mes: meses anteriores a um YYYYMM.
- variaveis_do_mes: meses do trimestre em varios formatos, prontos para filtros SQL.
- normalizar_data_referencia, meses_do_trimestre, rotulo_trimestre: utilitarios.

Nenhuma funcao considera feriados: apenas o dia da semana.
"""

from datetime import datetime, date
import pandas as pd

# ============================================================
# 1. CALCULO DE DATAS ANTERIORES
# ============================================================

def avaliar_dia(fechamento, ultimo_dia, dias_validos=(1, 2, 3, 4, 5)):
    """Calcula os 3 dias validos de referencia de um processamento, considerando o fechamento do periodo.

    Regras (hoje = data do sistema; ultimo_dia = fim do periodo processado):
        1. fechamento="N" e hoje <= ultimo_dia (processamento diario normal):
           data_d1 = ultimo dia valido ANTES de hoje.
        2. fechamento="N" e hoje > ultimo_dia (periodo ja terminou):
           a contagem passa a partir de ultimo_dia, e data_d1 = ultimo dia valido
           ANTES de ultimo_dia (o proprio ultimo_dia fica reservado ao fechamento).
        3. fechamento="S" e hoje > ultimo_dia (fechamento):
           data_d1 = o proprio ultimo_dia (mesmo que nao seja dia valido); data_d2
           e data_d3 sao os dias validos anteriores a ele, para comparacao com as previas.
        fechamento="S" com hoje <= ultimo_dia gera ValueError (periodo ainda nao terminou).

    Os dias validos sao definidos apenas pelo dia da semana; feriados NAO sao considerados.

    Parametros:
        fechamento (str): "N" durante o processamento diario, "S" no fechamento do
            periodo (aceita minusculas).
        ultimo_dia (str | int): ultimo dia do periodo, "YYYYMMDD" (fim do mes,
            do trimestre etc.).
        dias_validos (iterable[int], opcional): dias da semana validos, no padrao
            isoweekday: 1=segunda ... 7=domingo. Padrao: (1, 2, 3, 4, 5).

    Retorna:
        tuple com 9 valores, nesta ordem:
            data_d1 (str): 1o dia valido de referencia, "YYYYMMDD".
            data_d2 (str): 2o dia valido anterior, "YYYYMMDD".
            data_d3 (str): 3o dia valido anterior, "YYYYMMDD".
            mes_atual (str): ano/mes de data_d1, "YYYYMM".
            dia_atual_formatado (str): data_d1 em "YYYY-MM-DD".
            dia_atual_formatado_mais_um (str): dia seguinte a data_d1, "YYYY-MM-DD"
                (util como limite superior exclusivo em filtros de data).
            ano (int), mes (int), dia (int): partes de data_d1.

    Levanta:
        ValueError: fechamento="S" antes do fim do periodo, ultimo_dia em formato
            invalido ou dias_validos sem nenhum dia entre 1 e 7.

    Exemplo (supondo hoje = segunda-feira, 2026-09-28):
        >>> avaliar_dia("N", "20260930")        # regra 1
        ('20260925', '20260924', '20260923', '202609', '2026-09-25', '2026-09-26', 2026, 9, 25)
        >>> avaliar_dia("N", "20260925")[0]     # regra 2: dia anterior ao ultimo_dia
        '20260924'
        >>> avaliar_dia("S", "20260925")[0]     # regra 3: trava no ultimo_dia
        '20260925'
    """
    # Define a data inicial com base nas condicoes
    from datetime import datetime, timedelta

    if not dias_validos or not set(dias_validos) & {1, 2, 3, 4, 5, 6, 7}:
        # sem nenhum dia valido entre 1 e 7 o laco abaixo nunca terminaria
        raise ValueError("dias_validos deve conter ao menos um dia entre 1 (segunda) e 7 (domingo).")
    fechamento = str(fechamento).strip().upper()
    ultimo_dia = str(ultimo_dia)

    hoje = datetime.today()
    ultimo_dia_data = datetime.strptime(ultimo_dia, "%Y%m%d")

    if int(hoje.strftime("%Y%m%d")) <= int(ultimo_dia) and fechamento == 'N':
        # Caso esteja dentro do mes
        print("Rodando o codigo normalmente...")
        data_inicial = hoje
    elif int(hoje.strftime("%Y%m%d")) > int(ultimo_dia) and fechamento == 'N':
        # Caso esteja apos o fechamento do mes
        print("Travando o dia atual como o ultimo dia do mes...")
        data_inicial = ultimo_dia_data
    elif int(hoje.strftime("%Y%m%d")) > int(ultimo_dia) and fechamento == 'S':
        # Caso esteja apos o fechamento do mes e o periodo esteja fechado
        print("Travando o dia atual como o ultimo dia do mes...")
        data_inicial = ultimo_dia_data
    else:
        raise ValueError("Condicao nao tratada.")

    dias_validos_anteriores = []

    if fechamento == 'N':
        data_atual = data_inicial - timedelta(days=1)  # Retrocede um dia
        while len(dias_validos_anteriores) < 3:  # precisamos de 3 dias validos anteriores
            if data_atual.isoweekday() in dias_validos:  # Verifica se o dia da semana esta nos dias validos
                dias_validos_anteriores.append(data_atual)  # acrescenta a lista o valor valido
            data_atual -= timedelta(days=1)  # Retrocede um dia
    elif fechamento == 'S':
        data_atual = data_inicial  # Nao retrocede um dia
        dias_validos_anteriores.append(data_atual)  # crava o primeiro dia como sendo o ultimo dia do periodo
        while len(dias_validos_anteriores) < 3:  # precisamos de 3 dias validos anteriores
            data_atual -= timedelta(days=1)  # Retrocede um dia
            if data_atual.isoweekday() in dias_validos:  # Verifica se o dia da semana esta nos dias validos
                dias_validos_anteriores.append(data_atual)  # acrescenta a lista o valor valido

    # Converte as datas para o formato numerico e string
    data_d1 = dias_validos_anteriores[0].strftime('%Y%m%d')
    data_d2 = dias_validos_anteriores[1].strftime('%Y%m%d')
    data_d3 = dias_validos_anteriores[2].strftime('%Y%m%d')
    mes_atual = dias_validos_anteriores[0].strftime('%Y%m')
    dia_atual_formatado = dias_validos_anteriores[0].strftime('%Y-%m-%d')
    dia_atual_formatado_mais_um = (dias_validos_anteriores[0] + timedelta(days=1)).strftime('%Y-%m-%d')
    ano = int(mes_atual) // 100
    mes = int(mes_atual) % 100
    dia = int(data_d1) % 100

    return data_d1, data_d2, data_d3, mes_atual, dia_atual_formatado, dia_atual_formatado_mais_um, ano, mes, dia


def avaliar_dia_hoje(fechamento, ultimo_dia, dias_validos=(1, 2, 3, 4, 5)):
    """Igual a avaliar_dia, mas tambem devolve o momento atual como primeiro item.

    Regras (hoje = data do sistema; ultimo_dia = fim do periodo processado):
        1. fechamento="N" e hoje <= ultimo_dia (processamento diario normal):
           data_d1 = ultimo dia valido ANTES de hoje.
        2. fechamento="N" e hoje > ultimo_dia (periodo ja terminou):
           a contagem passa a partir de ultimo_dia, e data_d1 = ultimo dia valido
           ANTES de ultimo_dia (o proprio ultimo_dia fica reservado ao fechamento).
        3. fechamento="S" e hoje > ultimo_dia (fechamento):
           data_d1 = o proprio ultimo_dia (mesmo que nao seja dia valido); data_d2
           e data_d3 sao os dias validos anteriores a ele, para comparacao com as previas.
        fechamento="S" com hoje <= ultimo_dia gera ValueError (periodo ainda nao terminou).

    Os dias validos sao definidos apenas pelo dia da semana; feriados NAO sao considerados.

    Parametros:
        fechamento (str): "N" durante o processamento diario, "S" no fechamento do
            periodo (aceita minusculas).
        ultimo_dia (str | int): ultimo dia do periodo, "YYYYMMDD" (fim do mes,
            do trimestre etc.).
        dias_validos (iterable[int], opcional): dias da semana validos, no padrao
            isoweekday: 1=segunda ... 7=domingo. Padrao: (1, 2, 3, 4, 5).

    Retorna:
        tuple com 10 valores: hoje (datetime.datetime, data e hora atuais) seguido
        dos 9 valores de avaliar_dia:
            data_d1 (str): 1o dia valido de referencia, "YYYYMMDD".
            data_d2 (str): 2o dia valido anterior, "YYYYMMDD".
            data_d3 (str): 3o dia valido anterior, "YYYYMMDD".
            mes_atual (str): ano/mes de data_d1, "YYYYMM".
            dia_atual_formatado (str): data_d1 em "YYYY-MM-DD".
            dia_atual_formatado_mais_um (str): dia seguinte a data_d1, "YYYY-MM-DD"
                (util como limite superior exclusivo em filtros de data).
            ano (int), mes (int), dia (int): partes de data_d1.

    Levanta:
        ValueError: fechamento="S" antes do fim do periodo, ultimo_dia em formato
            invalido ou dias_validos sem nenhum dia entre 1 e 7.

    Exemplo:
        >>> hoje, data_d1, data_d2, *_ = avaliar_dia_hoje("N", "20261231")
    """
    from datetime import datetime, timedelta

    if not dias_validos or not set(dias_validos) & {1, 2, 3, 4, 5, 6, 7}:
        # sem nenhum dia valido entre 1 e 7 o laco abaixo nunca terminaria
        raise ValueError("dias_validos deve conter ao menos um dia entre 1 (segunda) e 7 (domingo).")
    fechamento = str(fechamento).strip().upper()
    ultimo_dia = str(ultimo_dia)

    hoje = datetime.today()
    ultimo_dia_data = datetime.strptime(ultimo_dia, "%Y%m%d")

    if int(hoje.strftime("%Y%m%d")) <= int(ultimo_dia) and fechamento == 'N':
        # Caso esteja dentro do mes
        print("Rodando o codigo normalmente...")
        data_inicial = hoje
    elif int(hoje.strftime("%Y%m%d")) > int(ultimo_dia) and fechamento == 'N':
        # Caso esteja apos o fechamento do mes
        print("Travando o dia atual como o ultimo dia do mes...")
        data_inicial = ultimo_dia_data
    elif int(hoje.strftime("%Y%m%d")) > int(ultimo_dia) and fechamento == 'S':
        # Caso esteja apos o fechamento do mes e o periodo esteja fechado
        print("Travando o dia atual como o ultimo dia do mes...")
        data_inicial = ultimo_dia_data
    else:
        raise ValueError("Condicao nao tratada.")

    dias_validos_anteriores = []

    if fechamento == 'N':
        data_atual = data_inicial - timedelta(days=1)
        while len(dias_validos_anteriores) < 3:
            if data_atual.isoweekday() in dias_validos:
                dias_validos_anteriores.append(data_atual)
            data_atual -= timedelta(days=1)
    elif fechamento == 'S':
        data_atual = data_inicial
        dias_validos_anteriores.append(data_atual)
        while len(dias_validos_anteriores) < 3:
            data_atual -= timedelta(days=1)
            if data_atual.isoweekday() in dias_validos:
                dias_validos_anteriores.append(data_atual)

    # Converte as datas para o formato numerico e string
    data_d1 = dias_validos_anteriores[0].strftime('%Y%m%d')
    data_d2 = dias_validos_anteriores[1].strftime('%Y%m%d')
    data_d3 = dias_validos_anteriores[2].strftime('%Y%m%d')
    mes_atual = dias_validos_anteriores[0].strftime('%Y%m')
    dia_atual_formatado = dias_validos_anteriores[0].strftime('%Y-%m-%d')
    dia_atual_formatado_mais_um = (dias_validos_anteriores[0] + timedelta(days=1)).strftime('%Y-%m-%d')
    ano = int(mes_atual) // 100
    mes = int(mes_atual) % 100
    dia = int(data_d1) % 100

    return hoje, data_d1, data_d2, data_d3, mes_atual, dia_atual_formatado, dia_atual_formatado_mais_um, ano, mes, dia


def variaveis_do_mes(anomes):
    """Gera, a partir de um ano/mes, as variaveis do trimestre-calendario que o contem, prontas para filtros.

    Parametros:
        anomes (str | int): ano e mes no formato YYYYMM (ex.: "202308" ou 202308).

    Retorna:
        tuple com 14 valores, nesta ordem (exemplo para anomes="202308"):
            meses_tri (str): "202307,202308,202309"
            mes_tri (list[int]): [7, 8, 9]
            datas_cadastro (list[int]): [202307, 202308, 202309]
            anomes_base_seg (list[str]): ["202307", "202308", "202309"]
            trimestre (int): 3
            mes_do_tri (int): posicao do mes no trimestre (1, 2 ou 3) -> 2
            mes1, mes2, mes3 (int): 202307, 202308, 202309
            mes1_traco, mes2_traco, mes3_traco (str): "2023-07", "2023-08", "2023-09"
            meses_tri_traco (str): "'2023-07-01','2023-08-01','2023-09-01'"
                (ja com aspas, para usar em "IN (...)" no SQL)
            primeiro_mes_tri_traco (str): "2023-07-01"

    Levanta:
        ValueError: se anomes nao tiver 6 digitos ou o mes nao estiver entre 01 e 12.

    Exemplo:
        >>> meses_tri, *_, meses_tri_traco, primeiro = variaveis_do_mes("202308")
        >>> f"WHERE dt_ref IN ({meses_tri_traco})"
        "WHERE dt_ref IN ('2023-07-01','2023-08-01','2023-09-01')"
    """
    meses_tri_traco = None
    anomes = str(anomes).strip()  # aceita int (202301) ou str ("202301")

    if len(anomes) != 6 or not anomes.isdigit() or anomes[4:] not in (
        "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"
    ):
        raise ValueError(f"anomes deve estar no formato YYYYMM com mes entre 01 e 12. Recebido: {anomes!r}")

    if anomes[4:] in ("01", "02", "03"):
        meses_tri = f"{anomes[0:4]}01,{anomes[0:4]}02,{anomes[0:4]}03"
        mes_tri = [1, 2, 3]
        ano = int(anomes[0:4])
        datas_cadastro = [(int(anomes[0:4]) * 100 + 1), (int(anomes[0:4]) * 100 + 2), (int(anomes[0:4]) * 100 + 3)]
        anomes_base_seg = [str(int(anomes[0:4]) * 100 + 1), str(int(anomes[0:4]) * 100 + 2), str(int(anomes[0:4]) * 100 + 3)]
        trimestre = 1
        mes1 = ano * 100 + 1
        mes2 = ano * 100 + 2
        mes3 = ano * 100 + 3
        mes1_traco = str(ano) + "-" + "01"
        mes2_traco = str(ano) + "-" + "02"
        mes3_traco = str(ano) + "-" + "03"
        if anomes[4:] == "01":
            mes_do_tri = 1
        elif anomes[4:] == "02":
            mes_do_tri = 2
        elif anomes[4:] == "03":
            mes_do_tri = 3

    elif anomes[4:] in ("04", "05", "06"):
        meses_tri = f"{anomes[0:4]}04,{anomes[0:4]}05,{anomes[0:4]}06"
        mes_tri = [4, 5, 6]
        ano = int(anomes[0:4])
        datas_cadastro = [(int(anomes[0:4]) * 100 + 4), (int(anomes[0:4]) * 100 + 5), (int(anomes[0:4]) * 100 + 6)]
        anomes_base_seg = [str(int(anomes[0:4]) * 100 + 4), str(int(anomes[0:4]) * 100 + 5), str(int(anomes[0:4]) * 100 + 6)]
        trimestre = 2
        mes1 = ano * 100 + 4
        mes2 = ano * 100 + 5
        mes3 = ano * 100 + 6
        mes1_traco = str(ano) + "-" + "04"
        mes2_traco = str(ano) + "-" + "05"
        mes3_traco = str(ano) + "-" + "06"
        if anomes[4:] == "04":
            mes_do_tri = 1
        elif anomes[4:] == "05":
            mes_do_tri = 2
        elif anomes[4:] == "06":
            mes_do_tri = 3

    elif anomes[4:] in ("07", "08", "09"):
        meses_tri = f"{anomes[0:4]}07,{anomes[0:4]}08,{anomes[0:4]}09"
        mes_tri = [7, 8, 9]
        ano = int(anomes[0:4])
        datas_cadastro = [(int(anomes[0:4]) * 100 + 7), (int(anomes[0:4]) * 100 + 8), (int(anomes[0:4]) * 100 + 9)]
        anomes_base_seg = [str(int(anomes[0:4]) * 100 + 7), str(int(anomes[0:4]) * 100 + 8), str(int(anomes[0:4]) * 100 + 9)]
        trimestre = 3
        mes1 = ano * 100 + 7
        mes2 = ano * 100 + 8
        mes3 = ano * 100 + 9
        mes1_traco = str(ano) + "-" + "07"
        mes2_traco = str(ano) + "-" + "08"
        mes3_traco = str(ano) + "-" + "09"
        if anomes[4:] == "07":
            mes_do_tri = 1
        elif anomes[4:] == "08":
            mes_do_tri = 2
        elif anomes[4:] == "09":
            mes_do_tri = 3

    elif anomes[4:] in ("10", "11", "12"):
        meses_tri = f"{anomes[0:4]}10,{anomes[0:4]}11,{anomes[0:4]}12"
        mes_tri = [10, 11, 12]
        ano = int(anomes[0:4])
        datas_cadastro = [(int(anomes[0:4]) * 100 + 10), (int(anomes[0:4]) * 100 + 11), (int(anomes[0:4]) * 100 + 12)]
        anomes_base_seg = [str(int(anomes[0:4]) * 100 + 10), str(int(anomes[0:4]) * 100 + 11), str(int(anomes[0:4]) * 100 + 12)]
        trimestre = 4
        mes1 = ano * 100 + 10
        mes2 = ano * 100 + 11
        mes3 = ano * 100 + 12
        mes1_traco = str(ano) + "-" + "10"
        mes2_traco = str(ano) + "-" + "11"
        mes3_traco = str(ano) + "-" + "12"
        if anomes[4:] == "10":
            mes_do_tri = 1
        elif anomes[4:] == "11":
            mes_do_tri = 2
        elif anomes[4:] == "12":
            mes_do_tri = 3

    # Define meses_tri_traco com base no trimestre
    if trimestre == 1:
        meses_tri_traco = f"'{ano}-01-01','{ano}-02-01','{ano}-03-01'"
    elif trimestre == 2:
        meses_tri_traco = f"'{ano}-04-01','{ano}-05-01','{ano}-06-01'"
    elif trimestre == 3:
        meses_tri_traco = f"'{ano}-07-01','{ano}-08-01','{ano}-09-01'"
    elif trimestre == 4:
        meses_tri_traco = f"'{ano}-10-01','{ano}-11-01','{ano}-12-01'"

    primeiro_mes_tri_traco = meses_tri_traco.split(",")[0].strip("'")

    return meses_tri, mes_tri, datas_cadastro, anomes_base_seg, trimestre, mes_do_tri, mes1, mes2, mes3, mes1_traco, mes2_traco, mes3_traco, meses_tri_traco, primeiro_mes_tri_traco


def avaliar_mes(anomes=None):
    """Retorna o ano/mes de referencia e os 3 meses anteriores, no formato YYYYMM.

    Parametros:
        anomes (str | int, opcional): mes de referencia, YYYYMM. Se None (ou vazio),
            usa o mes atual.

    Retorna:
        tuple[str, str, str, str]: (anomes_atual, anomes_ant, anomes_2_ant, anomes_3_ant).
        A virada de ano e tratada automaticamente.

    Levanta:
        ValueError: se anomes nao estiver no formato YYYYMM.

    Exemplo:
        >>> avaliar_mes(202301)
        ('202301', '202212', '202211', '202210')
    """
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    # Verifica se o parametro "anomes" foi fornecido
    if anomes:
        # Converte o valor fornecido para uma data
        data_atual = datetime.strptime(str(anomes), "%Y%m")
    else:
        # Usa a data atual
        data_atual = datetime.today()

    # Calcula os meses anteriores usando relativedelta
    mes_anterior = data_atual - relativedelta(months=1)
    segundo_mes_anterior = data_atual - relativedelta(months=2)
    terceiro_mes_anterior = data_atual - relativedelta(months=3)

    # Formata as datas no formato YYYYMM
    anomes_atual = data_atual.strftime('%Y%m')
    anomes_ant = mes_anterior.strftime('%Y%m')
    anomes_2_ant = segundo_mes_anterior.strftime('%Y%m')
    anomes_3_ant = terceiro_mes_anterior.strftime('%Y%m')

    return anomes_atual, anomes_ant, anomes_2_ant, anomes_3_ant


def avaliar_dia_diario():
    """Retorna a data de hoje e os 3 dias uteis anteriores (segunda a sexta, sem feriados).

    Se hoje for sabado ou domingo, o 1o dia util anterior e a sexta-feira.

    Retorna:
        tuple com 9 valores, nesta ordem:
            hoje (str): data atual, "YYYYMMDD".
            data_d1 (str): 1o dia util anterior a hoje, "YYYYMMDD".
            data_d2 (str): 2o dia util anterior, "YYYYMMDD".
            data_d3 (str): 3o dia util anterior, "YYYYMMDD".
            mes_atual (str): mes de HOJE, "YYYYMM".
            dia_atual_formatado (str): HOJE em "YYYY-MM-DD".
            ano (int), mes (int), dia (int): partes de HOJE.

    Atencao: diferente de avaliar_dia, aqui mes_atual, dia_atual_formatado, ano,
    mes e dia se referem a HOJE, e nao ao dia util anterior.

    Exemplo (supondo hoje = segunda-feira, 2026-09-28):
        >>> avaliar_dia_diario()
        ('20260928', '20260925', '20260924', '20260923', '202609', '2026-09-28', 2026, 9, 28)
    """
    from datetime import datetime
    import pandas as pd
    from pandas.tseries.offsets import BDay

    # Obtem a data atual
    data_atual = datetime.today()

    # Dias uteis anteriores com BDay (seg-sex). Em sabado/domingo, o 1o dia util
    # anterior e a sexta-feira.
    hoje_normalizado = pd.Timestamp(data_atual).normalize()
    dia_util_anterior = hoje_normalizado - BDay(1)
    segundo_dia_util_anterior = hoje_normalizado - BDay(2)
    terceiro_dia_util_anterior = hoje_normalizado - BDay(3)

    # Converte as datas para o formato numerico YYYYMMDD
    hoje = data_atual.strftime('%Y%m%d')
    data_d1 = dia_util_anterior.strftime('%Y%m%d')
    data_d2 = segundo_dia_util_anterior.strftime('%Y%m%d')
    data_d3 = terceiro_dia_util_anterior.strftime('%Y%m%d')

    # Extrai informacoes adicionais da data atual
    mes_atual = data_atual.strftime('%Y%m')
    dia_atual_formatado = data_atual.strftime('%Y-%m-%d')

    ano = int(mes_atual) // 100
    mes = int(mes_atual) % 100
    dia = int(hoje) % 100  # resto de dividir por 100 sera os dois ultimos digitos

    return hoje, data_d1, data_d2, data_d3, mes_atual, dia_atual_formatado, ano, mes, dia

# ============================================================
# 2. UTILITARIOS DE DATA E TRIMESTRE
# ============================================================

def normalizar_data_referencia(data_referencia=None, timezone="America/Sao_Paulo"):
    """Converte diferentes representacoes de data em datetime.date.

    Parametros:
        data_referencia (None | date | datetime | pd.Timestamp | str | int, opcional):
            - None: data de hoje no fuso "timezone";
            - datetime com fuso: convertido para "timezone" antes de pegar a data;
            - datetime sem fuso: usa a data como veio;
            - str/int: "YYYY-MM-DD" ou "YYYYMMDD" (ex.: "2026-09-27", 20260927).
        timezone (str, opcional): fuso IANA. Padrao: "America/Sao_Paulo".

    Retorna:
        datetime.date

    Levanta:
        ValueError: se o valor nao puder ser interpretado como data.

    Exemplo:
        >>> normalizar_data_referencia("20260927")
        datetime.date(2026, 9, 27)
    """
    from zoneinfo import ZoneInfo
    tz = ZoneInfo(timezone)
    if data_referencia is None:
        return datetime.now(tz).date()
    if isinstance(data_referencia, pd.Timestamp):
        data_referencia = data_referencia.to_pydatetime()
    if isinstance(data_referencia, datetime):
        if data_referencia.tzinfo is None:
            return data_referencia.date()  # datetime "ingenuo": usa a data como veio
        return data_referencia.astimezone(tz).date()
    if isinstance(data_referencia, date):
        return data_referencia
    if isinstance(data_referencia, int):
        data_referencia = str(data_referencia)
    if isinstance(data_referencia, str):
        texto = data_referencia.strip()
        for formato in ("%Y-%m-%d", "%Y%m%d"):
            try:
                return datetime.strptime(texto, formato).date()
            except ValueError:
                continue
    raise ValueError("data_referencia deve ser None, date, datetime ou string no formato YYYY-MM-DD / YYYYMMDD.")


def meses_do_trimestre(mes):
    """Retorna os numeros dos 3 meses do trimestre-calendario que contem "mes".

    Parametros:
        mes (int): mes de 1 a 12.

    Retorna:
        list[int]: ex.: 8 -> [7, 8, 9].
    """
    q = (mes - 1) // 3
    return [q * 3 + 1, q * 3 + 2, q * 3 + 3]


def rotulo_trimestre(ano, mes):
    """Monta o rotulo curto do trimestre, no formato "<trimestre>T<ano com 2 digitos>".

    Parametros:
        ano (int | str): ano com 4 digitos (ex.: 2026).
        mes (int): mes de 1 a 12.

    Retorna:
        str: ex.: (2026, 8) -> "3T26".
    """
    q = (mes - 1) // 3 + 1
    return f"{q}T{str(ano)[-2:]}"
