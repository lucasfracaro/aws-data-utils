"""certificados - Monta e mantem um arquivo cacert.crt para AWS CLI/boto3 em redes com proxy que inspeciona HTTPS.

Em redes que inspecionam HTTPS, o proxy apresenta um certificado emitido por
uma autoridade interna, instalada no Windows mas desconhecida do Python. O
arquivo gerado aqui junta o bundle publico do certifi com os certificados
confiaveis do Windows, e pode ser usado via variavel AWS_CA_BUNDLE.

Uso tipico:
    >>> from certificados import configurar_cacert_aws_se_necessario
    >>> r = configurar_cacert_aws_se_necessario()
    >>> # depois, uma vez no terminal: setx AWS_CA_BUNDLE "%USERPROFILE%\\.aws\\cacert.crt"

Fora do Windows, apenas o bundle do certifi e usado (um aviso fica em "erros").
"""

import os
import re
import ssl
import shutil
import hashlib
from datetime import datetime
from pathlib import Path


# ============================================================
# FUNCOES AUXILIARES
# ============================================================
# Etapas usadas por configurar_cacert_aws_se_necessario:
# bundle do certifi + certificados do Windows, deduplicados por hash.

_PADRAO_PEM = re.compile(
    r"-----BEGIN CERTIFICATE-----\s+.+?\s+-----END CERTIFICATE-----", re.DOTALL
)


def obter_caminho_padrao_cacert_aws():
    """Retorna o caminho padrao do arquivo: <pasta do usuario>/.aws/cacert.crt.

    Retorna:
        pathlib.Path (ex.: C:\\Users\\<usuario>\\.aws\\cacert.crt no Windows).
    """
    return Path.home() / ".aws" / "cacert.crt"


def _hash_certificado_pem(pem):
    """Calcula o SHA-256 de um certificado PEM (sobre os bytes DER), usado para deduplicar certificados.

    Parametros:
        pem (str): bloco "-----BEGIN CERTIFICATE----- ... -----END CERTIFICATE-----".

    Retorna:
        str: hash hexadecimal.
    """
    der = ssl.PEM_cert_to_DER_cert(pem)
    return hashlib.sha256(der).hexdigest()


def montar_conteudo_cacert_aws(stores=("ROOT", "CA")):
    """Monta o conteudo do cacert.crt: bundle publico do certifi + certificados dos stores do Windows, sem duplicatas.

    Parametros:
        stores (tuple[str], opcional): stores do Windows lidos com
            ssl.enum_certificates. Padrao: ("ROOT", "CA").

    Retorna:
        dict com:
            conteudo_final (str): todos os certificados em PEM, um apos o outro;
            certificados_windows_lidos (int): certificados X.509 lidos do Windows;
            certificados_windows_adicionados (int): os que nao existiam no certifi;
            certificados_duplicados_ignorados (int): os que ja existiam;
            erros (list[dict]): falhas nao fatais, cada uma com "store", "etapa" e "erro".

    Levanta:
        RuntimeError: se nenhum certificado for obtido (nem do certifi, nem do Windows).

    Requer:
        certifi (pip install certifi).
    """
    erros = []
    hashes = set()
    blocos = []

    # 1. Bundle publico do certifi
    try:
        import certifi
        with open(certifi.where(), "r", encoding="utf-8") as f:
            conteudo_certifi = f.read()
        for pem in _PADRAO_PEM.findall(conteudo_certifi):
            try:
                h = _hash_certificado_pem(pem)
            except Exception:
                continue
            if h not in hashes:
                hashes.add(h)
                blocos.append(pem.strip())
    except Exception as erro:
        erros.append({"store": "certifi", "etapa": "leitura_bundle", "erro": str(erro)})

    # 2. Certificados confiaveis do Windows (ssl.enum_certificates so existe no Windows)
    lidos = 0
    adicionados = 0
    duplicados = 0
    if not hasattr(ssl, "enum_certificates"):
        erros.append({"store": None, "etapa": "leitura_windows",
                      "erro": "ssl.enum_certificates indisponivel (sistema nao e Windows)."})
    else:
        for store in stores:
            try:
                certificados = ssl.enum_certificates(store)
            except Exception as erro:
                erros.append({"store": store, "etapa": "enum_certificates", "erro": str(erro)})
                continue
            for cert_bytes, encoding, _trust in certificados:
                if encoding != "x509_asn":
                    continue
                lidos += 1
                try:
                    h = hashlib.sha256(cert_bytes).hexdigest()
                    if h in hashes:
                        duplicados += 1
                        continue
                    pem = ssl.DER_cert_to_PEM_cert(cert_bytes)
                    hashes.add(h)
                    blocos.append(pem.strip())
                    adicionados += 1
                except Exception as erro:
                    erros.append({"store": store, "etapa": "conversao_pem", "erro": str(erro)})

    if not blocos:
        raise RuntimeError(f"Nenhum certificado foi obtido para montar o cacert.crt. Erros: {erros}")

    return {
        "conteudo_final": "\n".join(blocos) + "\n",
        "certificados_windows_lidos": lidos,
        "certificados_windows_adicionados": adicionados,
        "certificados_duplicados_ignorados": duplicados,
        "erros": erros,
    }


def arquivo_precisa_atualizar(cert_path, conteudo_novo):
    """Indica se o cacert.crt precisa ser (re)gravado.

    Compara o conteudo atual com o novo ignorando diferencas de quebra de linha
    (\\r\\n x \\n) e espacos nas pontas.

    Parametros:
        cert_path (str | Path): caminho do arquivo atual.
        conteudo_novo (str): conteudo esperado (ex.: de montar_conteudo_cacert_aws).

    Retorna:
        bool: True se o arquivo nao existe, nao pode ser lido ou tem conteudo diferente.
    """
    cert_path = Path(cert_path)
    if not cert_path.exists():
        return True
    try:
        conteudo_atual = cert_path.read_text(encoding="utf-8")
    except Exception:
        return True

    def _normalizar(texto):
        """Padroniza quebras de linha e remove espacos nas pontas para a comparacao."""
        return texto.replace("\r\n", "\n").strip()

    return _normalizar(conteudo_atual) != _normalizar(conteudo_novo)


def gravar_cacert_com_backup(cert_path, conteudo_final, manter_backup=True, max_backups=5):
    """Grava o cacert.crt de forma segura, opcionalmente guardando uma copia do arquivo anterior.

    O conteudo e gravado primeiro em "<arquivo>.tmp" e so depois substitui o
    original (um arquivo pela metade nunca fica no lugar). Os backups tem o nome
    "<arquivo>.bak_YYYYMMDD_HHMMSS" e ficam na mesma pasta; os mais antigos alem
    de "max_backups" sao apagados.

    Parametros:
        cert_path (str | Path): caminho do cacert.crt (a pasta e criada se nao existir).
        conteudo_final (str): conteudo a gravar.
        manter_backup (bool, opcional): cria backup se o arquivo ja existir. Padrao: True.
        max_backups (int, opcional): quantidade de backups mantidos. Padrao: 5.
            Use 0 ou None para nao apagar nenhum backup antigo.

    Retorna:
        dict: {"arquivo_final": str, "backup": str | None}.
    """
    cert_path = Path(cert_path)
    cert_path.parent.mkdir(parents=True, exist_ok=True)

    caminho_backup = None
    if manter_backup and cert_path.exists():
        sufixo = datetime.now().strftime("%Y%m%d_%H%M%S")
        caminho_backup = cert_path.with_name(f"{cert_path.name}.bak_{sufixo}")
        shutil.copy2(cert_path, caminho_backup)

        backups = sorted(cert_path.parent.glob(f"{cert_path.name}.bak_*"))
        excedentes = backups[:-max_backups] if max_backups and max_backups > 0 else []
        for antigo in excedentes:
            try:
                antigo.unlink()
            except OSError:
                pass

    # grava em arquivo temporario e substitui, para nao deixar o cacert pela metade
    temporario = cert_path.with_name(cert_path.name + ".tmp")
    temporario.write_text(conteudo_final, encoding="utf-8", newline="\n")
    os.replace(temporario, cert_path)

    return {"arquivo_final": str(cert_path), "backup": str(caminho_backup) if caminho_backup else None}


# ============================================================
# FUNCAO PRINCIPAL
# ============================================================

def configurar_cacert_aws_se_necessario(
    cert_path=None,
    stores=("ROOT", "CA"),
    manter_backup=True,
    max_backups=5,
    forcar=False,
    simular=False,
    verbose=True,
):
    """Gera ou atualiza o cacert.crt somente quando necessario, com backup e resumo.

    Atualiza quando: o arquivo nao existe, o conteudo difere do esperado
    (certifi + certificados do Windows, deduplicados) ou forcar=True.

    A funcao NAO altera variaveis de ambiente. Para AWS CLI/boto3 usarem o
    arquivo, defina AWS_CA_BUNDLE apontando para ele; o resultado avisa se ela
    estiver ausente ou apontando para outro arquivo.

    Parametros:
        cert_path (str | Path, opcional): caminho do arquivo.
            Padrao: <pasta do usuario>/.aws/cacert.crt.
        stores (tuple[str], opcional): stores do Windows. Padrao: ("ROOT", "CA").
        manter_backup (bool, opcional): guarda o arquivo anterior. Padrao: True.
        max_backups (int, opcional): backups mantidos. Padrao: 5.
        forcar (bool, opcional): grava mesmo sem diferenca. Padrao: False.
        simular (bool, opcional): so informa o que faria, sem gravar. Padrao: False.
        verbose (bool, opcional): imprime o resumo (imprimir_resumo_cacert). Padrao: True.

    Retorna:
        dict com: status ("atualizado", "sem_alteracao" ou "simulacao"),
        arquivo_final, backup, executou_atualizacao, forcar, simular,
        precisava_atualizar, aws_ca_bundle, aviso_aws_ca_bundle,
        certificados_windows_lidos, certificados_windows_adicionados,
        certificados_duplicados_ignorados e erros.

    Levanta:
        RuntimeError: se nenhum certificado puder ser obtido.

    Exemplo:
        >>> r = configurar_cacert_aws_se_necessario(simular=True)
        >>> r["status"], r["precisava_atualizar"]
    """
    if cert_path is None:
        cert_path = obter_caminho_padrao_cacert_aws()
    else:
        cert_path = Path(cert_path)

    dados_conteudo = montar_conteudo_cacert_aws(stores=stores)
    conteudo_final = dados_conteudo["conteudo_final"]

    precisa_atualizar = arquivo_precisa_atualizar(
        cert_path=cert_path,
        conteudo_novo=conteudo_final,
    )

    executar_atualizacao = forcar or precisa_atualizar

    aws_ca_bundle = os.environ.get("AWS_CA_BUNDLE")
    aviso_aws_ca_bundle = None

    if aws_ca_bundle and Path(aws_ca_bundle).resolve() != Path(cert_path).resolve():
        aviso_aws_ca_bundle = (
            "A variavel AWS_CA_BUNDLE esta apontando para um caminho diferente "
            "do arquivo configurado nesta execucao."
        )
    elif not aws_ca_bundle:
        aviso_aws_ca_bundle = (
            "A variavel AWS_CA_BUNDLE nao esta definida nesta sessao. "
            "Se AWS CLI/boto3 dependerem dela, configure apontando para o cacert.crt."
        )

    resultado = {
        "arquivo_final": str(cert_path),
        "backup": None,
        "status": None,
        "executou_atualizacao": False,
        "forcar": forcar,
        "simular": simular,
        "precisava_atualizar": precisa_atualizar,
        "aws_ca_bundle": aws_ca_bundle,
        "aviso_aws_ca_bundle": aviso_aws_ca_bundle,
        "certificados_windows_lidos": dados_conteudo["certificados_windows_lidos"],
        "certificados_windows_adicionados": dados_conteudo["certificados_windows_adicionados"],
        "certificados_duplicados_ignorados": dados_conteudo["certificados_duplicados_ignorados"],
        "erros": dados_conteudo["erros"],
    }

    if not executar_atualizacao:
        resultado["status"] = "sem_alteracao"
        if verbose:
            imprimir_resumo_cacert(resultado)
        return resultado

    if simular:
        resultado["status"] = "simulacao"
        resultado["executou_atualizacao"] = False
        if verbose:
            imprimir_resumo_cacert(resultado)
        return resultado

    dados_gravacao = gravar_cacert_com_backup(
        cert_path=cert_path,
        conteudo_final=conteudo_final,
        manter_backup=manter_backup,
        max_backups=max_backups,
    )

    resultado["status"] = "atualizado"
    resultado["executou_atualizacao"] = True
    resultado["arquivo_final"] = dados_gravacao["arquivo_final"]
    resultado["backup"] = dados_gravacao["backup"]

    if verbose:
        imprimir_resumo_cacert(resultado)

    return resultado


def imprimir_resumo_cacert(resultado: dict):
    """Imprime um resumo legivel do resultado de configurar_cacert_aws_se_necessario.

    Parametros:
        resultado (dict): dicionario retornado por configurar_cacert_aws_se_necessario.

    Retorna:
        None
    """
    status = resultado.get("status")

    print("=" * 70)
    print("CONFIGURACAO DO CERTIFICADO AWS")
    print("=" * 70)

    if status == "atualizado":
        print("# Arquivo cacert.crt atualizado com sucesso.")
    elif status == "sem_alteracao":
        print(" @ Nenhuma alteracao necessaria. O cacert.crt ja esta atualizado.")
    elif status == "simulacao":
        print(" Simulacao concluida. Nenhuma alteracao foi gravada.")
    else:
        print(f"[!] Status: {status}")

    print(f"Arquivo Final: {resultado.get('arquivo_final')}")
    print(f"Backup: {resultado.get('backup')}")
    print(f"Executou atualizacao: {resultado.get('executou_atualizacao')}")
    print(f"Forcou atualizacao: {resultado.get('forcar')}")

    print("-" * 70)
    print(f"Certificados Windows lidos: {resultado.get('certificados_windows_lidos')}")
    print(f"Certificados Windows adicionados: {resultado.get('certificados_windows_adicionados')}")
    print(f"Duplicados ignorados: {resultado.get('certificados_duplicados_ignorados')}")

    aws_ca_bundle = resultado.get("aws_ca_bundle")
    print("-" * 70)
    print(f"AWS_CA_BUNDLE atual: {aws_ca_bundle}")

    if resultado.get("aviso_aws_ca_bundle"):
        print(f"  Aviso: {resultado.get('aviso_aws_ca_bundle')}")

    erros = resultado.get("erros") or []
    if erros:
        print("-" * 70)
        print("  Erros encontrados durante a leitura/conversao:")
        for erro in erros:
            print(
                f"  - Store: {erro.get('store')} | "
                f"Etapa: {erro.get('etapa')} | "
                f"Erro: {erro.get('erro')}"
            )

    print("=" * 70)
