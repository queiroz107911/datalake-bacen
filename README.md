# DataLake BACEN — Pipeline Serverless na AWS (Medallion)

Pipeline de dados **serverless** na AWS que ingere séries temporais públicas do
**Banco Central do Brasil** (SGS), processa em camadas (**Bronze → Silver → Gold**)
e disponibiliza os dados para consulta analítica via **Athena**. Toda a
infraestrutura é provisionada como código com **Terraform**.

> Este README é longo de propósito. A ideia é que você consiga ler com calma,
> anotar e replicar o projeto do zero, entendendo **o que** cada peça faz e
> principalmente **por que** foi feita assim.

---

## Índice

1. [O que é o projeto e qual problema resolve](#1-o-que-é-o-projeto-e-qual-problema-resolve)
2. [Como a arquitetura funciona](#2-como-a-arquitetura-funciona)
3. [Cada arquivo do projeto](#3-cada-arquivo-do-projeto)
4. [O código explicado — Lambda e Glue](#4-o-código-explicado--lambda-e-glue)
5. [Cada recurso Terraform](#5-cada-recurso-terraform)
6. [Como rodar localmente (testes)](#6-como-rodar-localmente-testes)
7. [Como fazer o deploy na AWS](#7-como-fazer-o-deploy-na-aws)
8. [Conceitos-chave](#8-conceitos-chave)
9. [O que aprender depois](#9-o-que-aprender-depois)

---

## 1. O que é o projeto e qual problema resolve

**Problema:** dados econômicos do BACEN (como a taxa SELIC e o IPCA) ficam numa
API pública, em JSON, sem nenhuma organização para análise. Se você quiser fazer
análises históricas, cruzar séries ou rodar SQL em cima disso, precisa de um
lugar estruturado, barato e que se atualize sozinho.

**Solução:** um data lake na AWS, 100% serverless (você não administra nenhum
servidor), que:

- Coleta os dados automaticamente em um agendamento (sem ninguém apertar botão).
- Guarda o dado bruto fielmente (para auditoria e reprocessamento).
- Limpa e organiza o dado em formato eficiente para análise (Parquet).
- Disponibiliza tudo para consulta SQL via Athena, pagando só pelo que consulta.

**Séries usadas (SGS/BACEN):**

| Código | Série | Endpoint |
|---|---|---|
| `12`  | Taxa SELIC | `https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados?formato=json` |
| `433` | IPCA       | `https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados?formato=json` |

A API não exige autenticação e retorna uma lista de objetos `{"data": "DD/MM/AAAA", "valor": "x.yz"}`.

---

## 2. Como a arquitetura funciona

```
            ┌──────────────┐
            │  EventBridge │   (agendamento: cron)
            │   (Schedule) │
            └──────┬───────┘
                   │ dispara
                   ▼
            ┌──────────────┐      grava JSON bruto
            │    Lambda    │ ───────────────────────┐
            │  (ingestão)  │                          ▼
            └──────┬───────┘                  ┌───────────────┐
                   │ start_job_run            │  S3 — BRONZE  │
                   ▼                          │  (JSON cru)   │
            ┌──────────────┐   lê Bronze      └───────┬───────┘
            │   Glue Job   │ ◀────────────────────────┘
            │ (transform)  │   escreve Parquet
            └──────┬───────┘ ────────────────┐
                   │                          ▼
                   │                  ┌───────────────┐
                   │                  │  S3 — SILVER  │
                   │                  │ (Parquet part)│
                   │                  └───────┬───────┘
                   ▼                          │ varre/cataloga
            ┌──────────────┐                  ▼
            │ Glue Crawler │ ────────▶ ┌───────────────┐
            │              │           │  Glue Data    │
            └──────────────┘           │   Catalog     │
                                       └───────┬───────┘
                                               │ consulta SQL
                                               ▼
                                       ┌───────────────┐
                                       │    Athena     │
                                       └───────────────┘
```

**Fluxo passo a passo:**

1. **EventBridge** dispara a **Lambda** no horário agendado (cron).
2. A **Lambda** chama a API do BACEN para cada série e grava o **JSON bruto** no
   bucket **S3 Bronze**, particionado por série e data de ingestão. Em seguida,
   dispara o Glue Job.
3. O **Glue Job** lê todos os JSONs da Bronze, **limpa e tipa** os dados (datas e
   valores), adiciona colunas de partição e escreve em **Parquet** no bucket
   **S3 Silver**, particionado por `serie_codigo/ano/mes`.
4. O **Glue Crawler** varre a Silver e cria/atualiza tabelas no **Glue Data
   Catalog**, inferindo schema e partições.
5. O **Athena** consulta essas tabelas com SQL puro, lendo o Parquet direto do S3.

> **E a camada Gold?** Neste projeto, a "Gold" é representada pela camada de
> *consumo analítico*: as tabelas catalogadas e consultáveis no Athena sobre os
> dados Silver. Agregações específicas (ex.: média mensal por série) podem ser
> materializadas via `CREATE TABLE AS SELECT` (CTAS) no Athena — veja a
> [seção 9](#9-o-que-aprender-depois). Mantivemos o pipeline enxuto de propósito:
> complexidade só entra quando o problema pede.

---

## 3. Cada arquivo do projeto

```
datalake-bacen/
├── infra/                  → Infraestrutura como código (Terraform)
│   ├── main.tf             → provider AWS, versões, locals, workgroup Athena
│   ├── variables.tf        → variáveis configuráveis (região, séries, cron...)
│   ├── outputs.tf          → valores expostos após o apply
│   ├── s3.tf               → 4 buckets S3 + segurança + upload do script Glue
│   ├── iam.tf              → roles e policies (menor privilégio)
│   ├── lambda.tf           → empacotamento (.zip) e função Lambda + log group
│   ├── glue.tf             → Glue Job, Database e Crawler
│   └── eventbridge.tf      → regra de agendamento + permissão de invocação
├── lambda/
│   ├── handler.py          → função Lambda de ingestão (Bronze)
│   └── requirements.txt    → (vazio) documenta que não há deps a empacotar
├── glue/
│   └── transform_job.py    → script do Glue Job (Bronze → Silver)
├── tests/
│   ├── unit/
│   │   └── test_transform.py   → testes das funções puras de transformação
│   └── integration/
│       └── test_pipeline.py    → testa Lambda → S3 (com mocks/moto)
├── conftest.py             → adiciona lambda/ e glue/ ao sys.path do pytest
├── pyproject.toml          → projeto + dependências de dev (gerenciado por uv)
├── requirements-dev.txt    → espelho pip das deps de dev (alternativa ao uv)
├── .env.example            → modelo de variáveis de ambiente para uso local
├── .gitignore              → ignora venv, .env, estado do Terraform, etc.
└── README.md               → este arquivo
```

**Por que essa separação?** Cada pasta tem uma responsabilidade única:
`infra/` é só infraestrutura, `lambda/` e `glue/` são código de aplicação, e
`tests/` valida o comportamento. Misturar tudo numa pasta só seria mais difícil
de navegar e de dar manutenção.

---

## 4. O código explicado — Lambda e Glue

### 4.1 `lambda/handler.py` — ingestão (Bronze)

**Responsabilidade única:** buscar os dados na API e gravar o JSON bruto na
Bronze. Ele *não* transforma nada — fidelidade ao dado original é o objetivo da
camada Bronze.

Decisões de design e o porquê:

- **Só biblioteca padrão para HTTP (`urllib`).** A Lambda não usa `requests`. Por
  quê? Porque `requests` é uma dependência externa que precisaria ser empacotada
  no `.zip` de deploy (ou virar uma Layer). Usando `urllib` (que já vem no
  Python) + `boto3` (que já vem pré-instalado no runtime da Lambda), o pacote de
  deploy vira **só o `handler.py`**. Menos coisas para empacotar, menos coisas
  para quebrar.

- **Clientes boto3 criados *dentro* do handler**, não no topo do módulo. Isso
  facilita testes (o `moto` consegue interceptar a criação do cliente) e segue a
  boa prática de criar recursos no escopo de execução.

- **Configuração via variáveis de ambiente** (`os.environ`), nunca hardcoded. Na
  AWS, essas variáveis são definidas pelo Terraform (`lambda.tf`).

- **`_get_required_env`** falha de forma explícita (`KeyError`) se faltar uma
  variável obrigatória. É melhor estourar na hora, com mensagem clara, do que
  carregar um `None` que vai quebrar lá na frente, difícil de debugar.

- **`fetch_serie`** usa `timeout` na chamada HTTP (nunca chame API externa sem
  timeout!), trata `HTTPError` (status 4xx/5xx) e `URLError` (rede) separadamente,
  loga e **relança** (`raise`). Também valida que a resposta é uma lista — se o
  contrato da API mudar, falhamos explicitamente em vez de gravar lixo.

- **`montar_chave_s3`** define o layout da Bronze:
  `sgs/serie_codigo=12/ingestion_date=2026-06-23/dados.json`. Esse particionamento
  por série e data mantém o histórico de cada coleta e facilita reprocessamento.

- **`lambda_handler`** orquestra: lê config, itera as séries, busca e grava cada
  uma. Se qualquer série falhar, loga *qual* falhou e **relança** — preferimos
  falha explícita a uma ingestão silenciosamente incompleta. No fim, se
  `GLUE_JOB_NAME` estiver configurado, dispara o Glue Job (fluxo end-to-end).

### 4.2 `glue/transform_job.py` — transformação (Bronze → Silver)

**Responsabilidade única:** ler o JSON bruto, limpar/tipar e gravar Parquet
particionado.

A grande decisão de arquitetura aqui é a **separação entre lógica pura e I/O**:

- As funções no topo do módulo (`parse_records`, `_converter_data`,
  `_converter_valor`, `_remover_invalidos`, `adicionar_particoes`, `transformar`)
  são **puras**: só dependem de `pandas`. Não tocam em AWS, rede nem disco. Isso
  as torna **fáceis de testar** com pytest, sem precisar de Spark nem credenciais.

- Toda a parte de AWS (`_ler_bronze`, `_baixar_objeto`, `_escrever_silver`,
  `main`) fica embaixo, e os imports de `awsglue`/`awswrangler` ficam **dentro**
  das funções. Por quê dentro e não no topo? Porque essas bibliotecas só existem
  no ambiente do Glue. Se estivessem no topo do módulo, o simples `import
  transform_job` nos testes já quebraria. Colocando o import dentro de `main()`,
  o módulo importa normalmente em qualquer lugar.

Detalhes importantes:

- **Por que Python Shell e não Spark?** As séries do SGS são pequenas (centenas a
  poucos milhares de pontos). Spark seria desperdício de recurso, custo e
  complexidade. *A tecnologia é consequência do problema.*

- **`errors="coerce"`** em `pd.to_datetime` e `pd.to_numeric`: valores inválidos
  viram `NaT`/`NaN` em vez de estourar exceção. A limpeza (remoção desses nulos)
  acontece depois, de forma controlada, em `_remover_invalidos`.

- **Formato de data `%d/%m/%Y`**: a API retorna `DD/MM/AAAA`. Sem informar o
  formato, o pandas pode interpretar errado (ex.: confundir dia com mês).

- **Particionamento por `serie_codigo/ano/mes`**: deixa as consultas do Athena
  mais baratas e rápidas (*partition pruning* — o Athena lê só as partições
  necessárias em vez de varrer tudo).

- **`mode="overwrite_partitions"`** no `awswrangler`: reprocessar uma partição
  substitui só ela, sem duplicar dados. Reexecuções ficam idempotentes por
  partição.

---

## 5. Cada recurso Terraform

> Convenção de nomes: todos os recursos usam o prefixo
> `${project_name}-${environment}` (ex.: `datalake-bacen-dev`). Os buckets levam
> também o **account_id** como sufixo, porque nomes de bucket S3 são únicos em
> toda a AWS (não só na sua conta).

### `main.tf`
- **`terraform { required_version, required_providers }`**: trava versões mínimas
  do Terraform e do provider AWS, garantindo reprodutibilidade.
- **`provider "aws"`**: define a região e aplica `default_tags` a todos os
  recursos (governança: rastrear projeto, ambiente e dono).
- **`data "aws_caller_identity"`**: descobre o `account_id` da conta atual, usado
  para nomear buckets de forma globalmente única.
- **`locals`**: centraliza a convenção de nomes (prefixo e nomes dos buckets),
  evitando inconsistência entre os arquivos.
- **`aws_athena_workgroup`**: agrupa as consultas e define o local S3 dos
  resultados. Sem um local de resultados, o Athena recusa executar queries.

### `s3.tf`
- **`aws_s3_bucket` (bronze, silver, scripts, athena_results)**: os quatro
  buckets, cada um com função única (aterrissagem, dado confiável, script do
  Glue, resultados de query).
- **`aws_s3_bucket_public_access_block`**: bloqueia *qualquer* acesso público.
  Data lake nunca deve ser público por padrão.
- **`aws_s3_bucket_versioning`**: protege contra sobrescrita/exclusão acidental.
- **`aws_s3_bucket_server_side_encryption_configuration`**: criptografia em
  repouso (AES256) — padrão mínimo de segurança.
- **`aws_s3_object "glue_script"`**: faz upload do `transform_job.py` para o
  bucket de scripts; o `etag = filemd5(...)` força re-upload quando o arquivo muda.

### `iam.tf` — princípio do **menor privilégio**
- **Role da Lambda** (`aws_iam_role.lambda`): assumível pelo serviço Lambda.
  Permissões: `s3:PutObject` só na Bronze, `glue:StartJobRun` só no job deste
  projeto, e logs no CloudWatch. Nada de `Resource: *` solto.
- **Role do Glue** (`aws_iam_role.glue`): assumível pelo serviço Glue, usada pelo
  Job e pelo Crawler. Recebe a policy gerenciada `AWSGlueServiceRole` (permissões
  básicas do serviço) + uma policy específica de S3: ler Bronze e o script,
  ler/escrever a Silver.

### `lambda.tf`
- **`data "archive_file"`**: zipa o `handler.py` em tempo de `plan/apply` (sem
  build de dependências, porque não há nenhuma).
- **`aws_lambda_function`**: a função em si. `handler = "handler.lambda_handler"`
  (arquivo.função). `source_code_hash` força novo deploy quando o código muda.
  As variáveis de ambiente (`BRONZE_BUCKET`, `SGS_SERIES`, `GLUE_JOB_NAME`) são
  injetadas aqui.
- **`aws_cloudwatch_log_group`**: cria o log group com retenção de 14 dias. Se
  não criássemos, a Lambda criaria um com retenção infinita (custo acumulando).

### `glue.tf`
- **`aws_glue_job "transform"`**: job Python Shell apontando para o script no S3.
  `default_arguments` passa `--BRONZE_PATH`/`--SILVER_PATH` (lidos pelo script) e
  `--additional-python-modules = awswrangler==...` (instala o awswrangler na hora,
  já que ele não vem no Python Shell por padrão).
- **`aws_glue_catalog_database`**: o banco lógico no Data Catalog. O nome troca
  hífen por `_` porque nomes de database/tabela no Glue não aceitam hífen.
- **`aws_glue_crawler "silver"`**: varre a Silver, infere schema e partições, e
  cria tabelas com prefixo `silver_` no catálogo.

### `eventbridge.tf`
- **`aws_cloudwatch_event_rule`**: a regra com a expressão cron de agendamento.
- **`aws_cloudwatch_event_target`**: liga a regra à Lambda (define o alvo).
- **`aws_lambda_permission`**: **essencial** — sem ela, o EventBridge não
  consegue invocar a Lambda (falha de permissão silenciosa).

### `outputs.tf`
Expõe nomes dos buckets, da Lambda, do Glue Job, do database, do crawler e do
workgroup — úteis para conferência e para os comandos manuais de deploy/execução.

---

## 6. Como rodar localmente (testes)

> **Importante (WSL):** o projeto vive no filesystem do WSL
> (`/home/queiroz/datalake_bacen`). Rode os comandos **dentro do WSL/Ubuntu**, não
> pelo Python do Windows acessando o caminho de rede — bibliotecas com extensões
> nativas (numpy/pandas) não carregam DLL a partir de caminhos UNC `\\wsl.localhost\...`.

Este projeto usa **`uv`** como gerenciador de pacotes.

```bash
# 1. Instalar as dependências de desenvolvimento (cria o .venv e instala tudo)
uv sync

# 2. Rodar todos os testes
uv run pytest

# (opcional) rodar só os unitários ou só os de integração
uv run pytest tests/unit
uv run pytest tests/integration
```

Saída esperada: **9 passed**.

- **Testes unitários** (`tests/unit/test_transform.py`): exercitam as funções
  puras de transformação — tipagem de colunas, conversão de valores, remoção de
  inválidos, schema estável com lista vazia, partições e o pipeline completo.
- **Testes de integração** (`tests/integration/test_pipeline.py`): exercitam o
  handler da Lambda de ponta a ponta, com a API mockada (`unittest.mock`) e o S3
  simulado em memória (`moto`). Validam que os objetos certos são gravados na
  Bronze e que erros da API são propagados.

---

## 7. Como fazer o deploy na AWS

### Pré-requisitos
- Conta AWS com credenciais configuradas (`aws configure` ou variáveis de
  ambiente `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_DEFAULT_REGION`).
- **Terraform** >= 1.5 instalado.
- Permissões para criar S3, IAM, Lambda, Glue, EventBridge e Athena.

### Passo a passo

```bash
cd infra

# 1. Inicializa o Terraform (baixa o provider AWS)
terraform init

# 2. Confere o plano (o que será criado, sem aplicar nada)
terraform plan

# 3. Cria toda a infraestrutura na AWS
terraform apply
# (digite "yes" para confirmar)
```

Ao final, o Terraform imprime os **outputs** (nomes dos buckets, da Lambda, do
Glue Job, etc.). Anote-os.

### Executar o pipeline manualmente (primeira vez)

O EventBridge roda no agendamento, mas você pode disparar tudo na mão para testar:

```bash
# 1. Invocar a Lambda de ingestão (grava na Bronze e dispara o Glue Job)
aws lambda invoke \
  --function-name $(terraform output -raw lambda_function_name) \
  resposta.json
cat resposta.json

# 2. (Se preferir rodar o Glue Job manualmente)
aws glue start-job-run --job-name $(terraform output -raw glue_job_name)

# 3. Rodar o Crawler para catalogar a Silver
aws glue start-crawler --name $(terraform output -raw glue_crawler_name)
```

### Consultar no Athena

No console do Athena (ou via CLI), selecione o **workgroup** e o **database** do
projeto e rode, por exemplo:

```sql
-- Últimos valores da SELIC (série 12)
SELECT data, valor, ano, mes
FROM silver_sgs
WHERE serie_codigo = '12'
ORDER BY data DESC
LIMIT 10;
```

> O nome exato da tabela (`silver_sgs`) depende de como o crawler nomeia a pasta
> raiz. Confira no Glue Data Catalog após rodar o crawler.

### Destruir tudo (evitar custos)

```bash
cd infra
terraform destroy
```

---

## 8. Conceitos-chave

| Conceito | O que é | Por que neste projeto |
|---|---|---|
| **AWS Lambda** | Função que roda sob demanda, sem servidor para administrar. Você paga por execução. | Ingestão leve e esporádica (rodar 1x/dia). Não faz sentido manter um servidor ligado 24h para isso. |
| **AWS S3** | Armazenamento de objetos barato e durável. | É o "disco" do data lake — guarda Bronze (JSON) e Silver (Parquet). |
| **AWS Glue** | Serviço gerenciado de ETL. Aqui usamos o **Python Shell** (não Spark). | Transformar Bronze → Silver sem gerenciar infraestrutura. Python Shell é ideal para volume pequeno. |
| **Glue Crawler** | Varre dados no S3 e infere schema/partições, criando tabelas no catálogo. | Evita escrever DDL na mão — o crawler descobre as colunas e partições sozinho. |
| **Glue Data Catalog** | Metastore central: o "dicionário" de tabelas e schemas. | É o que o Athena consulta para saber onde e como estão os dados. |
| **AWS Athena** | Engine de SQL serverless que lê dados direto do S3. Paga por dados escaneados. | Permite análise com SQL puro sem montar banco de dados. |
| **AWS EventBridge** | Barramento de eventos / agendador (cron). | Dispara a Lambda no horário certo, sem intervenção manual. |
| **Parquet** | Formato colunar e comprimido. | Muito mais eficiente que JSON/CSV para análise — lê só as colunas necessárias e ocupa menos espaço (= Athena mais barato). |
| **Terraform** | Infraestrutura como código (declarativa). | Cria/destrói todo o ambiente de forma reprodutível e versionável. |
| **Arquitetura Medallion** | Camadas Bronze (cru) → Silver (limpo) → Gold (consumo). | Separa responsabilidades: fidelidade do dado, qualidade e consumo analítico. |

---

## 9. O que aprender depois

Evoluções naturais deste projeto, em ordem de complexidade:

1. **Camada Gold materializada:** criar agregações (ex.: média mensal por série)
   com `CREATE TABLE AS SELECT` (CTAS) no Athena, ou um segundo Glue Job
   Silver → Gold.
2. **Orquestração de verdade:** usar **Glue Workflows** ou **Step Functions** para
   encadear ingestão → transformação → crawler com tratamento de falhas e
   retentativas, em vez do disparo direto Lambda → Glue.
3. **Qualidade de dados:** validar schema e regras (ex.: `valor` não nulo, datas
   crescentes) com **Glue Data Quality** ou bibliotecas como `pandera`.
4. **Observabilidade:** alarmes no CloudWatch para falhas da Lambda/Glue e
   métricas de volume ingerido.
5. **Backend remoto do Terraform:** guardar o `tfstate` em um bucket S3 com lock
   no DynamoDB, para trabalhar em equipe sem corromper o estado.
6. **CI/CD:** pipeline (GitHub Actions) que roda `pytest` + `terraform plan` em
   cada PR e aplica no merge.
7. **Particionamento e formatos avançados:** estudar *Iceberg*/*Hudi* para
   *upserts* e *time travel* quando o volume e a necessidade de atualização
   crescerem.

---

### Resumo do que foi entregue

- ✅ Ingestão (Lambda) consumindo o SGS/BACEN e gravando JSON na Bronze.
- ✅ Transformação (Glue Python Shell) Bronze → Silver em Parquet particionado.
- ✅ Catalogação (Glue Crawler + Data Catalog) para consulta no Athena.
- ✅ Infraestrutura completa em Terraform (S3, IAM, Lambda, Glue, EventBridge, Athena).
- ✅ Testes unitários e de integração — **9 passando** com `uv run pytest`.
