# Utilitários Python para AWS (Glue, Athena, S3, Step Functions) e arquivos

Coleção de funções para rotinas de engenharia de dados: monitoramento de jobs AWS Glue, leitura/escrita no Athena, execução sequencial de Step Functions, operações no S3, cálculo de datas de referência, indexação/cópia de arquivos (inclusive dentro de .zip/.rar/.7z) e utilitários pandas/Spark.

Projeto pessoal, criado para automatizar tarefas que se repetem em pipelines de dados na AWS: conferir se os jobs rodaram, publicar tabelas no Athena e lidar com datas de referência.

Nenhuma função tem bucket, conta ou servidor fixo: esses valores são sempre passados por parâmetro.

## Módulos

| Arquivo | Conteúdo |
|---|---|
| `funcoes_glue.py` | Busca execuções de um Glue Job por data, filtra por payload, lê logs do CloudWatch e classifica cada execução (POSITIVO, FALSO_POSITIVO, FRACASSO, ALERTA…) |
| `funcoes_step_functions.py` | Executa uma lista de cargas numa Step Function, uma após a outra, aguardando cada uma |
| `funcoes_s3.py` | Verifica se um arquivo existe, grava tabela Spark como TXT único com cabeçalho, move e apaga prefixos |
| `datas.py` | Dias úteis anteriores, meses do trimestre, rótulo de trimestre, normalização de data de referência |
| `funcoes_arquivos.py` | Índice em Excel de uma pasta (com conteúdo de compactados), busca/cópia por filtro e criação de arquivos só com cabeçalho |
| `funcoes_excel.py` | Empilhar planilhas e dicionários de DataFrames, padronizar nomes de colunas |
| `funcoes_athena.py` | Leitura do Glue Catalog como view Spark, query Athena para pandas/Spark (com paginação) e `ParquetAthenaExporter`: DataFrame pandas → Parquet no S3 → tabela externa no Athena, com partições |
| `athena_export.py` | Converte TXT do S3 em Parquet particionado e cria/atualiza a tabela no Athena via Spark (`exporta_parquet_particionado_athena` recomendada) |
| `schema_normalize.py` | Normalizar DataFrames pandas e criar schemas Spark a partir do cabeçalho de arquivos TXT |
| `certificados.py` | Monta um `cacert.crt` (certifi + certificados do Windows) para AWS CLI/boto3 atrás de proxy com inspecao HTTPS |
| `utils_gerais.py` | Funções compartilhadas (sessão Spark, logger, valores vazios, impressão de tabelas) |

## Instalação

```bash
pip install -r requirements.txt
```

## Exemplos

```python
from funcoes_glue import avaliar_job_glue_por_lista_payloads_nomeados

resultado = avaliar_job_glue_por_lista_payloads_nomeados(
    nome_job="meu_job",
    data_referencia="2026-09-27",
    lista_payloads=[("Clientes", {"tablename": "clientes"})],
)
print(resultado["resumo"])
```

```python
import pandas as pd
from funcoes_athena import criar_tabela_athena_parquet

criar_tabela_athena_parquet(
    df=pd.DataFrame({"id": [1, 2], "dt": ["2026-09-27", "2026-09-27"]}),
    bucket_name="meu-bucket", pasta_no_bucket="dados/minha_tabela",
    database="meu_database", tabela="minha_tabela",
    profile_aws="meu-perfil", workgroup="meu-workgroup",
    caminho_parquet_local="saida/minha_tabela.parquet",
    partition_cols=["dt"], athena_results_prefix="resultados_athena/minha_tabela/",
)
```

```python
from datas import avaliar_dia_diario

hoje, data_d1, data_d2, data_d3, mes_atual, dia_fmt, ano, mes, dia = avaliar_dia_diario()
```

```python
from funcoes_arquivos import criar_indice_arquivos, buscar_e_copiar_arquivos

indice = criar_indice_arquivos("C:/dados")
encontrados = buscar_e_copiar_arquivos("vendas", "C:/saida", caminho_excel=indice)
```

## Observações

- Os arquivos importam uns aos outros pelo nome (`utils_gerais`, `datas`, `funcoes_s3`...), então devem ficar na mesma pasta. No AWS Glue, envie todos em `--extra-py-files`.
- Todas as funções têm docstring com parâmetros, retorno, exceções e exemplo: use `help(nome_da_funcao)` no Python.
- As funções que usam Spark reaproveitam a sessão já aberta (`SparkSession.builder.getOrCreate()`).

## Licença

MIT © 2026 Lucas Fracaro. Veja [LICENSE](LICENSE).
