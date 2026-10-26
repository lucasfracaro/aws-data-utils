"""funcoes_excel - Empilhamento de planilhas/DataFrames e padronizacao de nomes de colunas.
"""


def empilhar_excels(folder_path, name_part, sheet_name=None, usecols=None, rename_cols=None):
    """Le e empilha (concatena) as planilhas de uma pasta cujo nome contenha um texto.

    Para cada arquivo .xlsx/.xls em "folder_path" (em ordem alfabetica) cujo nome
    contenha "name_part" (sem diferenciar maiusculas):
        - le a aba "sheet_name" (ou a primeira);
        - mantem so as colunas "usecols", se informado;
        - adiciona a coluna "Origem" com o nome do arquivo.
    Arquivos que falharem na leitura sao ignorados (o erro e impresso).
    Subpastas nao sao percorridas.

    Parametros:
        folder_path (str): pasta com as planilhas.
        name_part (str): trecho que deve existir no nome do arquivo.
        sheet_name (str | int, opcional): nome ou indice da aba. Padrao: primeira aba.
        usecols (list | str, opcional): colunas a ler (mesmo formato do pandas.read_excel).
        rename_cols (dict, opcional): {nome_atual: novo_nome}, aplicado apos empilhar.

    Retorna:
        pandas.DataFrame: dados empilhados, ou DataFrame vazio se nenhum arquivo for encontrado.

    Requer:
        openpyxl (.xlsx) e xlrd (.xls).

    Exemplo:
        >>> df = empilhar_excels("relatorios", "vendas", sheet_name="Resumo",
        ...                      rename_cols={"Vlr": "valor"})
    """
    import os
    import pandas as pd

    dfs = []
    for filename in sorted(os.listdir(folder_path)):
        if filename.lower().endswith(('.xlsx', '.xls')) and name_part.lower() in filename.lower():
            caminho = os.path.join(folder_path, filename)
            try:
                df_temp = pd.read_excel(caminho, sheet_name=sheet_name or 0, usecols=usecols)
                df_temp["Origem"] = filename
                dfs.append(df_temp)
            except Exception as e:
                print(f"Erro ao ler {filename}: {e}")

    if not dfs:
        print("Nenhum arquivo encontrado com o criterio informado.")
        return pd.DataFrame()

    df_empilhado = pd.concat(dfs, ignore_index=True)

    if rename_cols:
        df_empilhado.rename(columns=rename_cols, inplace=True)

    return df_empilhado


def empilhar_dicionario_dataframes(
    dicionario_dataframes,
    usar="ultimo",
    adicionar_chave=True,
    validar_colunas=True,
    padronizar_ordem_colunas=True,
    usar_uniao_colunas=True,
    verbose=True,
):
    """Empilha os DataFrames de um dicionario de arquivos lidos em um unico DataFrame.

    Parametros:
        dicionario_dataframes (dict): no formato
            {"CHAVE": {"ultimo": DataFrame, "ocorrencias": [{"df": DataFrame, ...}, ...]}}
            (formato gerado por funcoes_arquivos.buscar_e_copiar_arquivos com
            criar_dicionario_dataframes=True)
        usar (str, opcional): "ultimo" empilha apenas o DataFrame "ultimo" de cada
            chave; "todas_ocorrencias" empilha todos de "ocorrencias". Padrao: "ultimo".
        adicionar_chave (bool, opcional): adiciona a coluna "chave_dicionario" com a
            chave de origem. Padrao: True.
        validar_colunas (bool, opcional): imprime um aviso com as colunas que nao
            existem em todas as chaves (nao interrompe). Padrao: True.
        padronizar_ordem_colunas (bool, opcional): ordena as colunas pela ordem em
            que aparecem pela primeira vez. Padrao: True.
        usar_uniao_colunas (bool, opcional): cria, com None, as colunas que faltam
            em cada DataFrame antes de empilhar. Padrao: True.
        verbose (bool, opcional): imprime avisos. Padrao: True.

    Retorna:
        pandas.DataFrame: dados empilhados (vazio se nao houver DataFrames).

    Levanta:
        ValueError: se "usar" nao for "ultimo" nem "todas_ocorrencias".

    Exemplo:
        >>> df_enc, dicionario = buscar_e_copiar_arquivos("vendas", "saida",
        ...                                               criar_dicionario_dataframes=True)
        >>> df_total = empilhar_dicionario_dataframes(dicionario)
    """
    import pandas as pd

    if usar not in ["ultimo", "todas_ocorrencias"]:
        raise ValueError('O parametro "usar" deve ser "ultimo" ou "todas_ocorrencias".')

    lista_dfs = []
    estruturas_ordem = {}
    estruturas_set = {}

    if usar == "ultimo":
        for chave, conteudo in dicionario_dataframes.items():
            df_temp = conteudo.get("ultimo")
            if df_temp is None:
                if verbose:
                    print(f'Chave ignorada sem DataFrame em "ultimo": {chave}')
                continue
            df_temp = df_temp.copy()
            if adicionar_chave:
                df_temp["chave_dicionario"] = chave
            estruturas_ordem[chave] = tuple(df_temp.columns)
            estruturas_set[chave] = frozenset(df_temp.columns)
            lista_dfs.append(df_temp)
    else:
        for chave, conteudo in dicionario_dataframes.items():
            ocorrencias = conteudo.get("ocorrencias", [])
            for ocorrencia in ocorrencias:
                df_temp = ocorrencia.get("df")
                if df_temp is None:
                    if verbose:
                        print(f"Ocorrencia ignorada sem DataFrame: {chave}")
                    continue
                df_temp = df_temp.copy()
                if adicionar_chave:
                    df_temp["chave_dicionario"] = chave
                estruturas_ordem[chave] = tuple(df_temp.columns)
                estruturas_set[chave] = frozenset(df_temp.columns)
                lista_dfs.append(df_temp)

    if not lista_dfs:
        if verbose:
            print("Nenhum DataFrame para empilhar.")
        return pd.DataFrame()

    if validar_colunas and len(estruturas_set) > 1:
        colunas_comuns = frozenset.intersection(*estruturas_set.values())
        colunas_diferentes = {}
        for chave, col_set in estruturas_set.items():
            diff = col_set.symmetric_difference(colunas_comuns)
            if diff:
                colunas_diferentes[chave] = diff
        if colunas_diferentes and verbose:
            print("Aviso: Estruturas de colunas diferentes detectadas:")
            for chave, diff in colunas_diferentes.items():
                print(f"  {chave}: {diff}")

    # ordem das colunas pela primeira aparicao
    todas_colunas = []
    for df in lista_dfs:
        for col in df.columns:
            if col not in todas_colunas:
                todas_colunas.append(col)

    if usar_uniao_colunas:
        for df in lista_dfs:
            for col in todas_colunas:
                if col not in df.columns:
                    df[col] = None

    if padronizar_ordem_colunas and todas_colunas:
        for i, df in enumerate(lista_dfs):
            # sem uniao de colunas, reordena apenas as colunas que o DataFrame possui
            lista_dfs[i] = df[[c for c in todas_colunas if c in df.columns]]

    df_empilhado = pd.concat(lista_dfs, ignore_index=True)
    return df_empilhado


def padronizar_colunas(df, mapa_alias):
    """Renomeia colunas de um DataFrame conforme um mapa de alias (atalho para df.rename(columns=...)).

    Parametros:
        df (pandas.DataFrame): DataFrame de entrada (nao e alterado).
        mapa_alias (dict): {nome_atual: novo_nome}. Colunas fora do mapa sao mantidas.

    Retorna:
        pandas.DataFrame: copia com as colunas renomeadas.

    Exemplo:
        >>> mapa = construir_mapa_alias([["cpf", "CPF", "nr_cpf"]])
        >>> df = padronizar_colunas(df, mapa)
    """

    df_resultado = df.rename(columns=mapa_alias)
    return df_resultado


def padronizar_colunas_com_log(df, mapa_alias, verbose=True):
    """Igual a padronizar_colunas, mas imprime quais colunas foram renomeadas e quais ficaram sem mapeamento.

    Parametros:
        df (pandas.DataFrame): DataFrame de entrada (nao e alterado).
        mapa_alias (dict): {nome_atual: novo_nome}.
        verbose (bool, opcional): imprime o log. Padrao: True.

    Retorna:
        pandas.DataFrame: copia com as colunas renomeadas.
    """

    colunas_originais = set(df.columns)
    colunas_mapeadas = set(mapa_alias.keys())
    colunas_nao_mapeadas = colunas_originais - colunas_mapeadas

    if verbose and colunas_nao_mapeadas:
        print(f"Colunas sem mapeamento (serao manteridas): {colunas_nao_mapeadas}")

    df_resultado = df.rename(columns=mapa_alias)

    if verbose:
        renomeadas = {k: v for k, v in mapa_alias.items() if k in colunas_originais}
        if renomeadas:
            print(f"Colunas renomeadas: {renomeadas}")

    return df_resultado


def diagnosticar_estruturas_dicionario(dicionario_dataframes, verbose=True):
    """Resume o conteudo de um dicionario de DataFrames (quantas chaves tem "ultimo", "ocorrencias" ou estao vazias).

    Parametros:
        dicionario_dataframes (dict): no formato
            {"CHAVE": {"ultimo": DataFrame, "ocorrencias": [{"df": DataFrame, ...}, ...]}}
            (formato gerado por funcoes_arquivos.buscar_e_copiar_arquivos com
            criar_dicionario_dataframes=True)
        verbose (bool, opcional): imprime o resumo. Padrao: True.

    Retorna:
        dict: com as chaves "total_chaves", "chaves_com_ultimo",
        "chaves_com_ocorrencias", "chaves_vazias" e "estruturas" (por chave:
        tem_ultimo, tem_ocorrencias, num_ocorrencias, colunas_ultimo e shape_ultimo).
    """
    import pandas as pd

    diagnostico = {
        "total_chaves": len(dicionario_dataframes),
        "chaves_com_ultimo": 0,
        "chaves_com_ocorrencias": 0,
        "chaves_vazias": 0,
        "estruturas": {},
    }

    for chave, conteudo in dicionario_dataframes.items():
        tem_ultimo = "ultimo" in conteudo and conteudo["ultimo"] is not None
        tem_ocorrencias = "ocorrencias" in conteudo and len(conteudo.get("ocorrencias", [])) > 0

        if tem_ultimo:
            diagnostico["chaves_com_ultimo"] += 1
        if tem_ocorrencias:
            diagnostico["chaves_com_ocorrencias"] += 1
        if not tem_ultimo and not tem_ocorrencias:
            diagnostico["chaves_vazias"] += 1

        estrutura = {
            "tem_ultimo": tem_ultimo,
            "tem_ocorrencias": tem_ocorrencias,
            "colunas_ultimo": None,
            "num_ocorrencias": len(conteudo.get("ocorrencias", [])),
        }

        if tem_ultimo and isinstance(conteudo["ultimo"], pd.DataFrame):
            estrutura["colunas_ultimo"] = list(conteudo["ultimo"].columns)
            estrutura["shape_ultimo"] = conteudo["ultimo"].shape

        diagnostico["estruturas"][chave] = estrutura

    if verbose:
        print("Diagnostico do dicionario de DataFrames:")
        print(f"  Total de chaves: {diagnostico['total_chaves']}")
        print(f"  Chaves com 'ultimo': {diagnostico['chaves_com_ultimo']}")
        print(f"  Chaves com 'ocorrencias': {diagnostico['chaves_com_ocorrencias']}")
        print(f"  Chaves vazias: {diagnostico['chaves_vazias']}")

    return diagnostico


def construir_mapa_alias(colunas_equivalentes):
    """Monta um mapa de renomeacao a partir de grupos de nomes equivalentes; o 1o nome de cada grupo e o padrao.

    Parametros:
        colunas_equivalentes (list[list[str]]): grupos de nomes que representam a
            mesma coluna. Grupos vazios sao ignorados.

    Retorna:
        dict: {nome_alternativo: nome_padrao}.

    Exemplo:
        >>> construir_mapa_alias([["cpf", "CPF", "nr_cpf"], ["valor", "vlr"]])
        {'CPF': 'cpf', 'nr_cpf': 'cpf', 'vlr': 'valor'}
    """
    mapa = {}
    for grupo in colunas_equivalentes:
        if not grupo:
            continue
        nome_padrao = grupo[0]
        for nome in grupo:
            if nome != nome_padrao:
                mapa[nome] = nome_padrao
    return mapa


def criar_mapa_renomeacao(colunas_equivalentes):
    """Sinonimo de construir_mapa_alias (mantido por compatibilidade).

    Parametros:
        colunas_equivalentes (list[list[str]]): grupos de nomes equivalentes; o 1o de cada grupo e o padrao.

    Retorna:
        dict: {nome_alternativo: nome_padrao}.
    """
    return construir_mapa_alias(colunas_equivalentes)
