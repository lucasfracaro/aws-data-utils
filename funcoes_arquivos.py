"""funcoes_arquivos - Indice, busca e copia de arquivos locais, inclusive dentro de .zip/.rar/.7z.

Fluxo tipico:
    1. criar_indice_arquivos: varre uma pasta (e o conteudo dos compactados) e
       grava um indice em Excel;
    2. buscar_e_copiar_arquivos: filtra o indice pelo nome, copia/extrai os
       arquivos encontrados e, opcionalmente, ja os le como DataFrames;
    3. criar_arquivos_zerados: gera copias contendo apenas o cabecalho
       (ex.: para montar massas de teste com a mesma estrutura).

Requer openpyxl. Para .rar instale rarfile (e o utilitario unrar); para .7z instale py7zr.
Sem esses pacotes, os compactados desses tipos sao ignorados na indexacao.
"""

import os
import io
import json
import shutil
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
from openpyxl import Workbook

try:
    import rarfile
except ImportError:
    rarfile = None

try:
    import py7zr
except ImportError:
    py7zr = None


# ============================================================
# AUXILIARES DE LEITURA DE ARQUIVOS (dentro ou fora de compactados)
# ============================================================

def _ler_bytes_de_comprimido(caminho_comprimido, arquivo_interno):
    """Le o conteudo (bytes) de um arquivo que esta dentro de um .zip, .rar ou .7z.

    Parametros:
        caminho_comprimido (str): caminho do arquivo compactado.
        arquivo_interno (str): caminho do arquivo dentro do compactado.

    Retorna:
        bytes

    Levanta:
        ImportError: .rar sem rarfile ou .7z sem py7zr.
        ValueError: extensao de compactado nao suportada.
        KeyError: se o arquivo interno nao existir no compactado.
    """
    nome_lower = str(caminho_comprimido).lower()
    if nome_lower.endswith(".zip"):
        with zipfile.ZipFile(caminho_comprimido, "r") as z:
            return z.read(arquivo_interno)
    if nome_lower.endswith(".rar"):
        if rarfile is None:
            raise ImportError("rarfile nao instalado: nao e possivel ler arquivos .rar")
        with rarfile.RarFile(caminho_comprimido, "r") as r:
            return r.read(arquivo_interno)
    if nome_lower.endswith(".7z"):
        if py7zr is None:
            raise ImportError("py7zr nao instalado: nao e possivel ler arquivos .7z")
        with py7zr.SevenZipFile(caminho_comprimido, mode="r") as z:
            dados = z.read([arquivo_interno])
            return dados[arquivo_interno].read()
    raise ValueError(f"Tipo de arquivo comprimido nao suportado: {caminho_comprimido}")


def _valor_preenchido(valor):
    """Retorna True se o valor nao e None/NaN nem texto vazio/"nan"/"None"/"<NA>"."""
    if valor is None:
        return False
    try:
        if pd.isna(valor):
            return False
    except (TypeError, ValueError):
        pass
    return str(valor).strip() not in {"", "nan", "None", "NaN", "<NA>"}


def _eh_verdadeiro(valor):
    """Interpreta bool, "true"/"1"/"sim"/"yes"/"verdadeiro" como True; demais valores como False."""
    if isinstance(valor, bool):
        return valor
    if not _valor_preenchido(valor):
        return False
    return str(valor).strip().lower() in {"true", "1", "sim", "yes", "verdadeiro"}


def _caminho_unico(caminho, sobrescrever):
    """Retorna "caminho" ou, se ele existir e sobrescrever=False, a primeira variacao livre "<nome>_1.ext", "<nome>_2.ext"...

    Parametros:
        caminho (str | Path): caminho desejado.
        sobrescrever (bool): se True, retorna o proprio caminho.

    Retorna:
        pathlib.Path
    """
    caminho = Path(caminho)
    if sobrescrever or not caminho.exists():
        return caminho
    contador = 1
    while True:
        novo = caminho.with_name(f"{caminho.stem}_{contador}{caminho.suffix}")
        if not novo.exists():
            return novo
        contador += 1

# ============================================================
# 15. INDICE E COPIA DE ARQUIVOS
# ============================================================

def criar_indice_arquivos(
    caminho_raiz, caminho_excel="resultado_arquivos.xlsx",
    caminho_cache="cache_arquivos_comprimidos.json",
    extensoes_comprimidas=(".zip", ".rar", ".7z"),
    incluir_arquivos_comprimidos_no_indice=True, verbose=True,
):
    """Varre uma pasta recursivamente e grava um indice de todos os arquivos em Excel.

    O conteudo dos compactados (.zip/.rar/.7z) tambem e listado. Para nao reabrir
    compactados grandes a cada execucao, a lista de arquivos internos fica em um
    cache JSON e so e relida quando a data de modificacao do compactado muda.
    Pastas sem permissao sao ignoradas (com aviso, se verbose=True).

    Colunas do indice (aba "Arquivos"):
        nome                nome do arquivo (sem pasta)
        caminho_completo    caminho no disco; para arquivo interno, o caminho DENTRO do compactado
        tamanho             bytes (descompactado, para arquivo interno)
        modificado_em       data de modificacao "YYYYMMDD" (do compactado, para arquivo interno)
        tipo_origem         "arquivo", "zip", "rar" ou "7z"
        caminho_comprimido  caminho do compactado (so para arquivo interno)
        arquivo_interno     caminho dentro do compactado (so para arquivo interno)
        eh_arquivo_interno  True se o arquivo esta dentro de um compactado

    Parametros:
        caminho_raiz (str): pasta a varrer.
        caminho_excel (str, opcional): onde gravar o indice. Padrao:
            "resultado_arquivos.xlsx" na pasta atual. Sobrescrito a cada execucao.
        caminho_cache (str, opcional): arquivo JSON de cache dos compactados.
            Padrao: "cache_arquivos_comprimidos.json" na pasta atual.
        extensoes_comprimidas (tuple[str], opcional): tipos de compactado cujo
            conteudo sera listado. Padrao: (".zip", ".rar", ".7z").
        incluir_arquivos_comprimidos_no_indice (bool, opcional): inclui tambem uma
            linha para o proprio compactado. Padrao: True.
        verbose (bool, opcional): imprime avisos e os caminhos gerados. Padrao: True.

    Retorna:
        str: caminho absoluto do Excel gerado.

    Exemplo:
        >>> indice = criar_indice_arquivos("C:/dados", caminho_excel="C:/indices/dados.xlsx")
    """
    caminho_raiz = os.path.abspath(caminho_raiz)
    caminho_excel = os.path.abspath(caminho_excel)
    caminho_cache = os.path.abspath(caminho_cache)

    def carregar_cache():
        if os.path.exists(caminho_cache):
            try:
                with open(caminho_cache, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def salvar_cache(cache):
        with open(caminho_cache, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)

    def formatar_data(timestamp):
        try:
            if timestamp is None:
                return None
            return datetime.fromtimestamp(timestamp).strftime("%Y%m%d")
        except Exception:
            return None

    def extrair_conteudo_zip(path_zip):
        arquivos = []
        with zipfile.ZipFile(path_zip, "r") as z:
            for info in z.infolist():
                if not info.is_dir():
                    arquivos.append({"nome": os.path.basename(info.filename), "arquivo_interno": info.filename, "tamanho": info.file_size})
        return arquivos

    def extrair_conteudo_rar(path_rar):
        if rarfile is None:
            return []
        arquivos = []
        with rarfile.RarFile(path_rar, "r") as r:
            for info in r.infolist():
                if not info.isdir():
                    arquivos.append({"nome": os.path.basename(info.filename), "arquivo_interno": info.filename, "tamanho": info.file_size})
        return arquivos

    def extrair_conteudo_7z(path_7z):
        if py7zr is None:
            return []
        arquivos = []
        with py7zr.SevenZipFile(path_7z, mode="r") as z:
            for info in z.list():
                if not info.is_directory:
                    arquivos.append({"nome": os.path.basename(info.filename), "arquivo_interno": info.filename, "tamanho": info.uncompressed})
        return arquivos

    def identificar_comprimido(nome_arquivo):
        nome_lower = nome_arquivo.lower()
        if nome_lower.endswith(".zip") and ".zip" in extensoes_comprimidas:
            return "zip", extrair_conteudo_zip
        if nome_lower.endswith(".rar") and ".rar" in extensoes_comprimidas:
            return "rar", extrair_conteudo_rar
        if nome_lower.endswith(".7z") and ".7z" in extensoes_comprimidas:
            return "7z", extrair_conteudo_7z
        return None, None

    cache_anterior = carregar_cache()
    cache_atualizado = {}
    wb = Workbook()
    ws = wb.active
    ws.title = "Arquivos"
    colunas = ["nome", "caminho_completo", "tamanho", "modificado_em", "tipo_origem", "caminho_comprimido", "arquivo_interno", "eh_arquivo_interno"]
    ws.append(colunas)

    def adicionar_linha_excel(ws, nome, caminho_completo, tamanho, modificado_em, tipo_origem, caminho_comprimido=None, arquivo_interno=None, eh_arquivo_interno=False):
        ws.append([nome, caminho_completo, tamanho, modificado_em, tipo_origem, caminho_comprimido, arquivo_interno, eh_arquivo_interno])

    def processar_arquivo_comprimido(caminho_comprimido, mtime, tipo, extrator):
        try:
            cache_antigo = cache_anterior.get(caminho_comprimido)
            if cache_antigo and abs(cache_antigo.get("modificado_em", 0) - mtime) < 1:
                arquivos_internos = cache_antigo.get("arquivos_internos", [])
            else:
                arquivos_internos = extrator(caminho_comprimido)
            for arq in arquivos_internos:
                nome_interno = arq.get("nome")
                arquivo_interno = arq.get("arquivo_interno", nome_interno)
                tamanho_interno = arq.get("tamanho")
                adicionar_linha_excel(ws=ws, nome=nome_interno, caminho_completo=arquivo_interno, tamanho=tamanho_interno, modificado_em=formatar_data(mtime), tipo_origem=tipo, caminho_comprimido=caminho_comprimido, arquivo_interno=arquivo_interno, eh_arquivo_interno=True)
            cache_atualizado[caminho_comprimido] = {"modificado_em": mtime, "tipo": tipo, "arquivos_internos": arquivos_internos}
        except Exception as e:
            print(f"Erro ao ler {tipo.upper()} {caminho_comprimido}: {e}")

    def scan(pasta):
        try:
            with os.scandir(pasta) as entradas:
                for entrada in entradas:
                    caminho_completo = entrada.path
                    nome = entrada.name
                    if entrada.is_file():
                        try:
                            stat = entrada.stat()
                            tamanho = stat.st_size
                            mtime = stat.st_mtime
                        except Exception:
                            tamanho = None
                            mtime = None
                        tipo, extrator = identificar_comprimido(nome)
                        if incluir_arquivos_comprimidos_no_indice or not tipo:
                            adicionar_linha_excel(ws=ws, nome=nome, caminho_completo=caminho_completo, tamanho=tamanho, modificado_em=formatar_data(mtime), tipo_origem=tipo if tipo else "arquivo")
                        if tipo and extrator and mtime is not None:
                            processar_arquivo_comprimido(caminho_comprimido=caminho_completo, mtime=mtime, tipo=tipo, extrator=extrator)
                    elif entrada.is_dir(follow_symlinks=False):
                        scan(caminho_completo)
        except PermissionError:
            if verbose:
                print(f"Sem permissao: {pasta}")
        except Exception as e:
            if verbose:
                print(f"Erro em {pasta}: {e}")

    scan(caminho_raiz)
    pasta_excel = os.path.dirname(caminho_excel)
    if pasta_excel:
        os.makedirs(pasta_excel, exist_ok=True)
    wb.save(caminho_excel)
    salvar_cache(cache_atualizado)
    if verbose:
        print(f"Excel gerado em: {caminho_excel}")
        print(f"Cache salvo em: {caminho_cache}")
    return caminho_excel


def buscar_e_copiar_arquivos(
    filtros, pasta_destino, caminho_excel="resultado_arquivos.xlsx", modo="OU",
    copiar=True, case_sensitive=True, sobrescrever=True, manter_estrutura_interna=False,
    retornar_apenas_arquivos=True, criar_dicionario_dataframes=False,
    sep="\t", encoding="utf-8", dtype=str, read_csv_kwargs=None,
    chave_dataframe="nome", verbose=True,
):
    """Busca arquivos no indice pelo nome e copia (ou extrai dos compactados) os encontrados para uma pasta.

    A busca e feita por trecho do nome do arquivo (coluna "nome"). Arquivos
    internos de compactados sao extraidos diretamente, sem descompactar o resto.

    Parametros:
        filtros (str | list[str]): trecho(s) procurados no nome. Uma string com
            virgulas e dividida em varios filtros ("vendas,2026" = ["vendas", "2026"]).
        pasta_destino (str): pasta para onde os arquivos serao copiados (criada se nao existir).
        caminho_excel (str, opcional): indice gerado por criar_indice_arquivos.
            Padrao: "resultado_arquivos.xlsx".
        modo (str, opcional): "OU" = o nome contem ALGUM filtro; "E" = contem TODOS. Padrao: "OU".
        copiar (bool, opcional): copia/extrai os arquivos. Se False, apenas retorna
            a lista encontrada. Padrao: True.
        case_sensitive (bool, opcional): diferencia maiusculas de minusculas. Padrao: True.
        sobrescrever (bool, opcional): sobrescreve arquivos existentes no destino.
            Atencao: arquivos encontrados com o MESMO nome em pastas diferentes se
            sobrescrevem (fica o ultimo). Use False para gerar "<nome>_1.ext"... Padrao: True.
        manter_estrutura_interna (bool, opcional): para arquivos internos, recria no
            destino as subpastas que existiam dentro do compactado. Padrao: False.
        retornar_apenas_arquivos (bool, opcional): mantido por compatibilidade (o
            indice so contem arquivos). Padrao: True.
        criar_dicionario_dataframes (bool, opcional): tambem le cada arquivo
            encontrado como DataFrame (.xlsx/.xls/.xlsm com read_excel; os demais
            com read_csv). Padrao: False.
        sep (str, opcional): separador para read_csv. Padrao: "\\t".
        encoding (str, opcional): encoding para read_csv. Padrao: "utf-8".
        dtype (type | dict, opcional): dtype para a leitura. Padrao: str (tudo texto).
        read_csv_kwargs (dict, opcional): argumentos extras para read_csv.
        chave_dataframe (str, opcional): coluna do indice usada como chave do
            dicionario. Padrao: "nome".
        verbose (bool, opcional): imprime o progresso. Padrao: True.

    Retorna:
        - se criar_dicionario_dataframes=False: pandas.DataFrame com as linhas
          encontradas do indice, mais ordem_localizacao e, se copiar=True,
          caminho_copiado e status_copia ("copiado" ou "erro: ...");
        - se criar_dicionario_dataframes=True: tuple (DataFrame acima, dicionario)
          no formato {chave: {"ultimo": DataFrame, "ocorrencias": [{"df", "caminho",
          "ordem_localizacao"}, ...]}}. Arquivos que nao puderem ser lidos ficam de fora.

    Levanta:
        FileNotFoundError: se o indice nao existir.
        ValueError: filtros vazios ou modo diferente de "OU"/"E".

    Exemplo:
        >>> df, dicionario = buscar_e_copiar_arquivos(
        ...     "vendas", "C:/saida", caminho_excel="C:/indices/dados.xlsx",
        ...     case_sensitive=False, criar_dicionario_dataframes=True, sep=";")
    """
    if read_csv_kwargs is None:
        read_csv_kwargs = {}
    caminho_excel = os.path.abspath(caminho_excel)
    pasta_destino = os.path.abspath(pasta_destino)

    if not os.path.exists(caminho_excel):
        raise FileNotFoundError(f"Excel de indice nao encontrado: {caminho_excel}")
    os.makedirs(pasta_destino, exist_ok=True)

    if isinstance(filtros, str):
        filtros = [f.strip() for f in filtros.split(",") if f.strip()]
    if not isinstance(filtros, list) or len(filtros) == 0:
        raise ValueError("O parametro filtros deve ser uma string ou uma lista com pelo menos um item.")

    modo = modo.upper().strip()
    if modo not in ["OU", "E"]:
        raise ValueError('O parametro modo deve ser "OU" ou "E".')

    df = pd.read_excel(caminho_excel)
    df["nome"] = df["nome"].astype(str)

    if retornar_apenas_arquivos:
        df_f = df[df["nome"].notna()].copy()
    else:
        df_f = df.copy()

    if case_sensitive:
        filtros_tratados = [str(f) for f in filtros]
        def normalizar_texto(txt):
            return str(txt)
    else:
        filtros_tratados = [str(f).lower() for f in filtros]
        def normalizar_texto(txt):
            return str(txt).lower()

    def contem_algum(nome):
        nome_tratado = normalizar_texto(nome)
        return any(filtro in nome_tratado for filtro in filtros_tratados)

    def contem_todos(nome):
        nome_tratado = normalizar_texto(nome)
        return all(filtro in nome_tratado for filtro in filtros_tratados)

    if modo == "OU":
        df_encontrados = df_f[df_f["nome"].apply(contem_algum)].copy()
    else:
        df_encontrados = df_f[df_f["nome"].apply(contem_todos)].copy()

    if df_encontrados.empty:
        if verbose:
            print(f"Nenhum arquivo encontrado para os filtros: {filtros}")
        if criar_dicionario_dataframes:
            return df_encontrados, {}
        return df_encontrados

    # Ordem em que os arquivos foram localizados (usada por criar_arquivos_zerados)
    df_encontrados = df_encontrados.reset_index(drop=True)
    df_encontrados["ordem_localizacao"] = range(1, len(df_encontrados) + 1)

    if verbose:
        print(f"Arquivos encontrados: {len(df_encontrados)}")

    # ------------------------------------------------------------------
    # Copia / extracao dos arquivos encontrados
    # ------------------------------------------------------------------
    caminhos_copiados = []
    status_copia = []
    if copiar:
        for _, row in df_encontrados.iterrows():
            nome = os.path.basename(str(row["nome"]))
            eh_interno = _eh_verdadeiro(row.get("eh_arquivo_interno"))
            try:
                if eh_interno:
                    caminho_zip = row.get("caminho_comprimido")
                    arquivo_interno = row.get("arquivo_interno")
                    if not (_valor_preenchido(caminho_zip) and _valor_preenchido(arquivo_interno)):
                        raise ValueError("Linha marcada como arquivo interno sem caminho_comprimido/arquivo_interno.")
                    arquivo_interno = str(arquivo_interno)
                    if manter_estrutura_interna:
                        partes = [p for p in arquivo_interno.replace("\\", "/").split("/") if p not in ("", ".", "..")]
                        destino = Path(pasta_destino).joinpath(*partes)
                    else:
                        destino = Path(pasta_destino) / nome
                    destino = _caminho_unico(destino, sobrescrever)
                    destino.parent.mkdir(parents=True, exist_ok=True)
                    destino.write_bytes(_ler_bytes_de_comprimido(str(caminho_zip), arquivo_interno))
                else:
                    origem = row.get("caminho_completo")
                    if not _valor_preenchido(origem) or not os.path.exists(str(origem)):
                        raise FileNotFoundError(f"Arquivo de origem nao encontrado: {origem}")
                    destino = _caminho_unico(Path(pasta_destino) / nome, sobrescrever)
                    if os.path.abspath(str(origem)) != os.path.abspath(str(destino)):
                        shutil.copy2(str(origem), str(destino))
                caminhos_copiados.append(str(destino))
                status_copia.append("copiado")
                if verbose:
                    print(f"  Copiado: {destino}")
            except Exception as erro:
                caminhos_copiados.append(None)
                status_copia.append(f"erro: {erro}")
                if verbose:
                    print(f"  Erro ao copiar {nome}: {erro}")
        df_encontrados["caminho_copiado"] = caminhos_copiados
        df_encontrados["status_copia"] = status_copia

    if not criar_dicionario_dataframes:
        return df_encontrados

    # ------------------------------------------------------------------
    # Dicionario no formato esperado por empilhar_dicionario_dataframes:
    # {chave: {"ultimo": DataFrame, "ocorrencias": [{"df": DataFrame, ...}]}}
    # ------------------------------------------------------------------
    dicionario = {}
    for _, row in df_encontrados.iterrows():
        caminho = row.get("caminho_copiado") if "caminho_copiado" in df_encontrados.columns else None
        if not _valor_preenchido(caminho) and not _eh_verdadeiro(row.get("eh_arquivo_interno")):
            caminho = row.get("caminho_completo")
        chave = str(row.get(chave_dataframe, row["nome"]))
        try:
            if _valor_preenchido(caminho):
                fonte = str(caminho)
            else:
                fonte = io.BytesIO(_ler_bytes_de_comprimido(str(row.get("caminho_comprimido")), str(row.get("arquivo_interno"))))
            extensao = Path(str(row["nome"])).suffix.lower()
            if extensao in (".xlsx", ".xls", ".xlsm"):
                df_lido = pd.read_excel(fonte, dtype=dtype)
            else:
                df_lido = pd.read_csv(fonte, sep=sep, encoding=encoding, dtype=dtype, **read_csv_kwargs)
        except Exception as erro:
            if verbose:
                print(f"  Nao foi possivel ler {row['nome']}: {erro}")
            continue
        item = dicionario.setdefault(chave, {"ultimo": None, "ocorrencias": []})
        item["ocorrencias"].append({"df": df_lido, "caminho": caminho, "ordem_localizacao": row["ordem_localizacao"]})
        item["ultimo"] = df_lido  # a ultima ocorrencia localizada prevalece

    return df_encontrados, dicionario


def criar_arquivos_zerados(
    df_arquivos, pasta_destino, zerar=True, usar="ultimo",
    coluna_agrupamento="nome", coluna_caminho="caminho_copiado",
    sobrescrever=True, adicionar_sufixo_ocorrencia=True,
    encodings_tentativa=("utf-8-sig", "utf-8", "cp1252", "latin1", "utf-16"),
    extensoes_texto=(".txt", ".csv", ".tsv", ".dat", ".log"),
    extensoes_excel=(".xlsx", ".xls"),
    read_excel_kwargs=None, verbose=True,
):
    """Cria no destino copias dos arquivos informados contendo apenas o cabecalho (ou copias completas, com zerar=False).

    Regras com zerar=True:
        - extensoes de texto: grava so a primeira linha, no mesmo encoding
          detectado (tenta, em ordem, os de encodings_tentativa);
        - Excel: grava so os nomes das colunas da primeira aba (.xls e salvo como .xlsx);
        - outras extensoes: copia o arquivo completo.

    Origem de cada arquivo: a coluna "coluna_caminho" (ex.: caminho_copiado), se
    preenchida; senao, o arquivo e lido de dentro do compactado (arquivos
    internos) ou de caminho_completo.

    Parametros:
        df_arquivos (pandas.DataFrame): resultado de buscar_e_copiar_arquivos ou
            linhas do indice de criar_indice_arquivos.
        pasta_destino (str): pasta onde os arquivos serao criados.
        zerar (bool, opcional): True = so cabecalho; False = copia completa. Padrao: True.
        usar (str, opcional): "ultimo" = para cada nome (coluna_agrupamento), so a
            ultima ocorrencia pela ordem_localizacao; "todas_ocorrencias" = todas.
            Padrao: "ultimo".
        coluna_agrupamento (str, opcional): coluna usada para agrupar ocorrencias. Padrao: "nome".
        coluna_caminho (str, opcional): coluna com o caminho fisico preferencial.
            Padrao: "caminho_copiado".
        sobrescrever (bool, opcional): sobrescreve arquivos existentes no destino;
            se False, usa "<nome>_1.ext"... Padrao: True.
        adicionar_sufixo_ocorrencia (bool, opcional): com usar="todas_ocorrencias",
            nomeia como "<nome>_ocorrencia_N.ext". Padrao: True.
        encodings_tentativa (tuple[str], opcional): encodings testados nos arquivos
            de texto. Padrao: ("utf-8-sig", "utf-8", "cp1252", "latin1", "utf-16").
        extensoes_texto (tuple[str], opcional): Padrao: (".txt", ".csv", ".tsv", ".dat", ".log").
        extensoes_excel (tuple[str], opcional): Padrao: (".xlsx", ".xls").
        read_excel_kwargs (dict, opcional): argumentos extras para pandas.read_excel
            (ex.: {"sheet_name": "Dados"}).
        verbose (bool, opcional): imprime o progresso. Padrao: True.

    Retorna:
        pandas.DataFrame: uma linha por arquivo, com as colunas nome, caminho_origem,
        caminho_comprimido, arquivo_interno, caminho_destino, eh_arquivo_interno,
        zerado, status ("sucesso"/"erro"), tipo_processamento, encoding_utilizado,
        mensagem, ordem_localizacao e ocorrencia_grupo. Erros em um arquivo nao
        interrompem os demais.

    Levanta:
        TypeError: se df_arquivos nao for um pandas.DataFrame.

    Exemplo:
        >>> df = buscar_e_copiar_arquivos("vendas", "C:/copias", caminho_excel=indice)
        >>> resultado = criar_arquivos_zerados(df, "C:/estrutura")
        >>> resultado[["nome", "status", "tipo_processamento"]]
    """
    if read_excel_kwargs is None:
        read_excel_kwargs = {}
    if not isinstance(df_arquivos, pd.DataFrame):
        raise TypeError("df_arquivos deve um pandas.DataFrame.")

    colunas_retorno = ["nome", "caminho_origem", "caminho_comprimido", "arquivo_interno", "caminho_destino", "eh_arquivo_interno", "zerado", "status", "tipo_processamento", "encoding_utilizado", "mensagem", "ordem_localizacao", "ocorrencia_grupo"]

    if df_arquivos.empty:
        return pd.DataFrame(columns=colunas_retorno)

    df_processar = df_arquivos.copy()
    df_processar = df_processar[df_processar["nome"].notna()].copy()
    df_processar["nome"] = df_processar["nome"].astype(str).str.strip()
    df_processar = df_processar[~df_processar["nome"].isin(["", "nan", "None", "NaN", "<NA>"])].copy()

    if df_processar.empty:
        if verbose:
            print("Nenhum arquivo valido para processar.")
        return pd.DataFrame(columns=colunas_retorno)

    def valor_vazio_local(valor):
        if valor is None:
            return True
        try:
            if pd.isna(valor):
                return True
        except (TypeError, ValueError):
            pass
        return str(valor).strip() in {"", "nan", "None"}

    def converter_para_bool(valor):
        if isinstance(valor, bool):
            return valor
        if valor_vazio_local(valor):
            return False
        valor_texto = str(valor).strip().lower()
        return valor_texto in {"true", "1", "sim", "yes", "verdadeiro"}

    def obter_caminho_fisico(row):
        if coluna_caminho in row.index:
            caminho_preferencial = row.get(coluna_caminho)
            if not valor_vazio_local(caminho_preferencial):
                return str(caminho_preferencial)
        caminho_original = row.get("caminho_completo")
        if not valor_vazio_local(caminho_original):
            return str(caminho_original)
        return None

    def obter_caminho_comprimido(row):
        caminho_zip = row.get("caminho_comprimido")
        if valor_vazio_local(caminho_zip):
            return None
        return str(caminho_zip)

    def obter_arquivo_interno(row):
        arquivo_interno = row.get("arquivo_interno")
        if valor_vazio_local(arquivo_interno):
            return None
        return str(arquivo_interno)

    def gerar_nome_destino(nome_original, ocorrencia=None):
        nome_original = os.path.basename(str(nome_original))
        caminho_nome = Path(nome_original)
        stem = caminho_nome.stem
        suffix = caminho_nome.suffix
        if usar == "todas_ocorrencias" and adicionar_sufixo_ocorrencia and ocorrencia is not None:
            nome_saida = f"{stem}_ocorrencia_{ocorrencia}{suffix}"
        else:
            nome_saida = nome_original
        return os.path.join(pasta_destino, nome_saida)

    def gerar_caminho_unico(caminho_destino):
        caminho_destino = Path(caminho_destino)
        if sobrescrever or not caminho_destino.exists():
            return str(caminho_destino)
        pasta = caminho_destino.parent
        stem = caminho_destino.stem
        suffix = caminho_destino.suffix
        contador = 1
        while True:
            novo_caminho = pasta / f"{stem}_{contador}{suffix}"
            if not novo_caminho.exists():
                return str(novo_caminho)
            contador += 1

    df_processar = df_processar.reset_index(drop=True)
    df_processar["_ordem_apos_filtros"] = range(len(df_processar))

    if "ordem_localizacao" in df_processar.columns:
        df_processar["_ordem_selecao"] = pd.to_numeric(df_processar["ordem_localizacao"], errors="coerce")
        df_processar["_ordem_selecao"] = df_processar["_ordem_selecao"].fillna(df_processar["_ordem_apos_filtros"])
    else:
        df_processar["_ordem_selecao"] = df_processar["_ordem_apos_filtros"]

    df_processar = df_processar.sort_values(by="_ordem_selecao", kind="stable").copy()
    df_processar["ocorrencia_grupo"] = df_processar.groupby(coluna_agrupamento, dropna=False).cumcount() + 1

    ultima_ordem = df_processar.groupby(coluna_agrupamento, dropna=False)["_ordem_selecao"].transform("max")
    df_processar["_eh_ultimo_apos_filtros"] = df_processar["_ordem_selecao"].eq(ultima_ordem)

    if usar == "ultimo":
        df_processar = df_processar[df_processar["_eh_ultimo_apos_filtros"]].copy()

    df_processar = df_processar.sort_values(by="_ordem_selecao", kind="stable").reset_index(drop=True)

    resultados = []
    total_selecionados = len(df_processar)
    if verbose:
        print("=" * 80)
        if zerar:
            print("CRIACAO DE ARQUIVOS SOMENTE COM CABECALHO")
        else:
            print("COPIA COMPLETA DOS ARQUIVOS")
        print("=" * 80)
        print(f"Modo de selecao: {usar}")
        print(f"Zerar arquivos: {zerar}")
        print(f"Arquivos selecionados: {total_selecionados}")
        print(f"Pasta destino: {pasta_destino}")
        print("=" * 80)

    for numero, (_, row) in enumerate(df_processar.iterrows(), start=1):
        nome = str(row["nome"])
        eh_arquivo_interno = converter_para_bool(row.get("eh_arquivo_interno"))
        caminho_fisico = obter_caminho_fisico(row)
        caminho_zip = obter_caminho_comprimido(row)
        arquivo_interno = obter_arquivo_interno(row)
        ordem_localizacao = row.get("ordem_localizacao")
        ocorrencia_grupo = row.get("ocorrencia_grupo")
        caminho_destino = gerar_nome_destino(nome_original=nome, ocorrencia=ocorrencia_grupo)
        caminho_destino = gerar_caminho_unico(caminho_destino)
        extensao = Path(nome).suffix.lower()

        resultado = {
            "nome": nome, "caminho_origem": None, "caminho_comprimido": caminho_zip,
            "arquivo_interno": arquivo_interno, "caminho_destino": caminho_destino,
            "eh_arquivo_interno": eh_arquivo_interno, "zerado": zerar,
            "status": None, "tipo_processamento": None, "encoding_utilizado": None,
            "mensagem": None, "ordem_localizacao": ordem_localizacao, "ocorrencia_grupo": ocorrencia_grupo,
        }

        try:
            if verbose:
                print(f"  [{numero}/{total_selecionados}] Processando: {nome}")

            # Arquivo interno sem copia fisica: le direto de dentro do compactado
            usar_comprimido = eh_arquivo_interno and caminho_zip and arquivo_interno and not (
                caminho_fisico and coluna_caminho in row.index and _valor_preenchido(row.get(coluna_caminho))
            )
            if usar_comprimido:
                resultado["caminho_origem"] = f"{caminho_zip}\\{arquivo_interno}"
                conteudo = _ler_bytes_de_comprimido(caminho_zip, arquivo_interno)
            else:
                if not caminho_fisico or not os.path.exists(caminho_fisico):
                    raise FileNotFoundError(f"Arquivo de origem nao encontrado: {caminho_fisico}")
                resultado["caminho_origem"] = caminho_fisico
                with open(caminho_fisico, "rb") as f:
                    conteudo = f.read()

            Path(caminho_destino).parent.mkdir(parents=True, exist_ok=True)

            if not zerar:
                with open(caminho_destino, "wb") as f:
                    f.write(conteudo)
                resultado["tipo_processamento"] = "copia_completa"
                resultado["mensagem"] = "Arquivo completo criado no destino."
            elif extensao in extensoes_texto:
                texto = None
                for enc in encodings_tentativa:
                    try:
                        texto = conteudo.decode(enc)
                        resultado["encoding_utilizado"] = enc
                        break
                    except (UnicodeDecodeError, LookupError):
                        continue
                if texto is None:
                    raise UnicodeDecodeError("multi", b"", 0, 1, f"nenhum encoding funcionou: {encodings_tentativa}")
                primeira_linha = texto.splitlines(keepends=True)[:1]
                cabecalho = primeira_linha[0] if primeira_linha else ""
                enc_saida = resultado["encoding_utilizado"]
                if enc_saida == "utf-8-sig":
                    enc_saida = "utf-8"  # o BOM ja veio no decode; evita duplicar
                with open(caminho_destino, "w", encoding=enc_saida, newline="") as f:
                    f.write(cabecalho)
                resultado["tipo_processamento"] = "texto_somente_cabecalho"
                resultado["mensagem"] = "Arquivo criado somente com o cabecalho."
            elif extensao in extensoes_excel:
                df_excel = pd.read_excel(io.BytesIO(conteudo), nrows=0, **read_excel_kwargs)
                caminho_saida = caminho_destino
                if extensao == ".xls":
                    # openpyxl nao grava .xls; salva como .xlsx
                    caminho_saida = str(Path(caminho_destino).with_suffix(".xlsx"))
                    resultado["caminho_destino"] = caminho_saida
                df_excel.to_excel(caminho_saida, index=False)
                resultado["tipo_processamento"] = "excel_somente_cabecalho"
                resultado["mensagem"] = "Arquivo Excel criado somente com o cabecalho."
            else:
                with open(caminho_destino, "wb") as f:
                    f.write(conteudo)
                resultado["tipo_processamento"] = "copia_completa_extensao_nao_suportada"
                resultado["mensagem"] = f"Extensao {extensao or '(sem extensao)'} nao suportada para zerar; arquivo copiado completo."

            resultado["status"] = "sucesso"
            if verbose:
                print(f"    Criado: {resultado['caminho_destino']}")
        except Exception as erro:
            resultado["status"] = "erro"
            resultado["mensagem"] = str(erro)
            if verbose:
                print(f"    Erro: {erro}")

        resultados.append(resultado)

    df_resultado = pd.DataFrame(resultados, columns=colunas_retorno)
    if verbose:
        total_sucesso = df_resultado["status"].eq("sucesso").sum()
        total_erros = df_resultado["status"].eq("erro").sum()
        print("=" * 80)
        print(f"Selecionados: {len(df_resultado)}")
        print(f"Criados com sucesso: {total_sucesso}")
        print(f"Erros: {total_erros}")
        print(f"Pasta destino: {pasta_destino}")
    return df_resultado
