# CNO Database

Pipeline ETL para baixar os dados públicos do Cadastro Nacional de Obras (CNO),
carregá-los no MotherDuck e enriquecer os responsáveis com informações de CNPJ
da [BrasilAPI](https://brasilapi.com.br/).

## Requisitos

- Python 3.10 ou superior
- uma conta no MotherDuck
- a variável de ambiente `MOTHERDUCK_TOKEN`

Instale as dependências:

```bash
python -m pip install -r requirements.txt
```

No PowerShell, configure o token apenas para a sessão atual:

```powershell
$env:MOTHERDUCK_TOKEN = "seu-token"
```

## Uso

O pipeline completo continua sendo o comportamento padrão:

```bash
python main.py
```

Também é possível executar uma etapa isoladamente:

```bash
python main.py extract
python main.py transform
python main.py cnpj
python main.py retry
```

Consulte todas as opções com `python main.py --help`. A URL pode ser alterada
por `--url` ou pela variável `CNO_URL`, sem editar o código.

Em automações recorrentes, `--allow-stale-raw` permite continuar com a última
carga bruta válida quando a Receita estiver temporariamente indisponível. O
fallback só é aceito se as tabelas `cno` e `cno_areas` já existirem.

## Configuração

| Variável | Padrão | Finalidade |
| --- | ---: | --- |
| `CNO_BATCH_SIZE` | `50` | CNPJs persistidos por transação |
| `CNO_MAX_WORKERS` | `10` | consultas simultâneas à BrasilAPI |
| `CNO_API_RETRIES` | `3` | tentativas por consulta nova |
| `CNO_DOWNLOAD_RETRIES` | `3` | tentativas de download completo |
| `CNO_CONNECT_TIMEOUT` | `30` | timeout de conexão em segundos |
| `CNO_READ_TIMEOUT` | `300` | timeout entre leituras em segundos |
| `CNO_FALLBACK_URLS` | vazio | URLs alternativas separadas por `|` |
| `CNO_BATCH_PAUSE` | `0.5` | pausa em segundos entre lotes |
| `CNO_LOG_LEVEL` | `INFO` | nível dos logs |
| `CNO_MAX_DOWNLOAD_BYTES` | `0` | limite opcional do ZIP; zero desabilita |
| `CNO_MAX_UNCOMPRESSED_BYTES` | `0` | limite opcional dos CSVs; zero desabilita |

## Confiabilidade da carga

- o download usa streaming, timeout, retry e validação de tamanho/ZIP;
- os CSVs são descompactados em blocos, sem ocupar seu tamanho inteiro na RAM;
- CSVs com encoding fora do suporte nativo do DuckDB sao recodificados para UTF-8 em streaming;
- todos os CSVs são carregados em tabelas de staging antes da troca transacional;
- nomes e valores usados no SQL são escapados;
- conexões, arquivos temporários e registros do Pandas são liberados mesmo em erro;
- lotes da BrasilAPI usam pool de conexões, backoff e transações;
- o reprocessamento substitui os dados de empresa e sócios de forma consistente.
