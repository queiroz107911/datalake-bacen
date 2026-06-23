"""Lambda de ingestão (camada Bronze) do pipeline DataLake BACEN.

Esta função é acionada pelo EventBridge em um agendamento. Para cada série
temporal configurada (SGS do Banco Central), ela:

1. Consome a API pública do BACEN (SGS).
2. Salva o JSON bruto, sem nenhuma transformação, no bucket S3 da camada Bronze.
3. (Opcional) Dispara o Glue Job de transformação Bronze -> Silver.

Decisões de design importantes:

- Usamos APENAS a biblioteca padrão (`urllib`) para a chamada HTTP. Isso evita
  ter que empacotar dependências (como `requests`) no pacote de deploy da Lambda.
  Menos dependência = deploy mais simples e menor superfície de falha.
- `boto3` já vem pré-instalado no runtime da Lambda, então não precisa ser
  empacotado.
- Os clientes boto3 são criados DENTRO do handler (lazy), não no topo do módulo.
  Isso facilita os testes (ex.: `moto` consegue interceptar) e segue o padrão de
  criar recursos dentro do escopo de execução.
- Toda configuração vem de variáveis de ambiente (`os.environ`), nunca hardcoded.
  Na Lambda, as variáveis de ambiente são definidas pelo Terraform.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import urllib.error
import urllib.request
from typing import Any

import boto3

# Logger no nível do módulo. Na Lambda, o handler raiz já está configurado,
# então basta pegar um logger nomeado e ajustar o nível.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# URL base da API do SGS (Sistema Gerenciador de Séries Temporais) do BACEN.
# O endpoint não requer autenticação.
SGS_URL_TEMPLATE = (
    "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json&dataInicial={data_inicial}&dataFinal={data_final}"
)

# Timeout (em segundos) para a chamada HTTP. Nunca chame uma API externa sem
# timeout — sem ele, a Lambda pode ficar pendurada até o timeout da própria
# Lambda, desperdiçando tempo de execução (e dinheiro).
HTTP_TIMEOUT_SECONDS = 30


def _get_required_env(nome: str) -> str:
    """Lê uma variável de ambiente obrigatória ou falha de forma explícita.
    Args:
        nome: nome da variável de ambiente.

    Returns:
        O valor da variável.

    Raises:
        KeyError: se a variável não estiver definida. Falhar cedo e com mensagem
            clara é melhor do que usar um valor `None` que estoura mais adiante.
    """
    try:
        return os.environ[nome]
    except KeyError:
        logger.error("Variável de ambiente obrigatória ausente: %s", nome)
        raise


def fetch_serie(codigo: str) -> list[dict[str, Any]]:
    """Busca uma série temporal na API do SGS do BACEN.

    Args:
        codigo: código numérico da série (ex.: "12" para SELIC, "433" para IPCA).

    Returns:
        Lista de registros no formato [{"data": "DD/MM/AAAA", "valor": "x.yz"}, ...].

    Raises:
        urllib.error.URLError: se a chamada HTTP falhar (rede, DNS, etc.).
        urllib.error.HTTPError: se a API retornar status 4xx/5xx.
        json.JSONDecodeError: se a resposta não for um JSON válido.
    """
    data_final = dt.date.today().strftime("%d/%m/%Y")
    data_inicial = (dt.date.today() - dt.timedelta(days=365)).strftime("%d/%m/%Y")
    url = SGS_URL_TEMPLATE.format(codigo=codigo, data_inicial=data_inicial, data_final=data_final)
    logger.info("Buscando série %s em %s", codigo, url)

    # User-Agent explícito: algumas APIs públicas bloqueiam clientes sem ele.
    requisicao = urllib.request.Request(
        url, headers={"User-Agent": "datalake-bacen/1.0", "Accept": "application/json"}
    )

    try:
        with urllib.request.urlopen(requisicao, timeout=HTTP_TIMEOUT_SECONDS) as resp:
            corpo = resp.read().decode("utf-8")
    except urllib.error.HTTPError as erro:
        # Erro de status HTTP (4xx/5xx). Logamos e relançamos para a Lambda falhar.
        logger.error("Erro HTTP %s ao buscar série %s", erro.code, codigo)
        raise
    except urllib.error.URLError as erro:
        # Erro de rede/conexão (DNS, timeout, recusa de conexão).
        logger.error("Erro de conexão ao buscar série %s: %s", codigo, erro.reason)
        raise

    dados = json.loads(corpo)

    if not isinstance(dados, list):
        # A API do SGS sempre retorna uma lista. Se vier outra coisa, é sinal de
        # mudança no contrato ou erro mascarado — falhamos explicitamente.
        raise ValueError(
            f"Resposta inesperada da API para a série {codigo}: esperava uma lista"
        )

    logger.info("Série %s retornou %d registros", codigo, len(dados))
    return dados


def montar_chave_s3(codigo: str, data_ingestao: dt.date) -> str:
    """Monta a chave (caminho) do objeto no S3 para a camada Bronze.

    A chave é particionada por código da série e por data de ingestão. Esse
    layout facilita reprocessamento e mantém o histórico de cada coleta:

        sgs/serie_codigo=12/ingestion_date=2026-06-23/dados.json

    Args:
        codigo: código da série.
        data_ingestao: data em que o dado foi coletado.

    Returns:
        A chave (key) do objeto no S3.
    """
    return (
        f"sgs/serie_codigo={codigo}/"
        f"ingestion_date={data_ingestao.isoformat()}/dados.json"
    )


def salvar_bronze(
    cliente_s3: Any, bucket: str, chave: str, dados: list[dict[str, Any]]
) -> None:
    """Salva os dados brutos como JSON no bucket Bronze do S3.

    Args:
        cliente_s3: cliente boto3 do S3.
        bucket: nome do bucket Bronze.
        chave: caminho (key) do objeto.
        dados: registros brutos vindos da API.

    Raises:
        botocore.exceptions.ClientError: se o `put_object` falhar.
    """
    corpo = json.dumps(dados, ensure_ascii=False).encode("utf-8")
    logger.info("Salvando %d bytes em s3://%s/%s", len(corpo), bucket, chave)
    cliente_s3.put_object(
        Bucket=bucket,
        Key=chave,
        Body=corpo,
        ContentType="application/json",
    )


def _disparar_glue_job(cliente_glue: Any, nome_job: str) -> None:
    """Dispara o Glue Job de transformação, se configurado.

    Mantém o fluxo end-to-end: assim que a ingestão termina, a transformação
    Bronze -> Silver é acionada. É opcional — controlado pela env var GLUE_JOB_NAME.

    Args:
        cliente_glue: cliente boto3 do Glue.
        nome_job: nome do Glue Job a iniciar.
    """
    logger.info("Disparando Glue Job: %s", nome_job)
    resposta = cliente_glue.start_job_run(JobName=nome_job)
    logger.info("Glue Job iniciado. JobRunId=%s", resposta.get("JobRunId"))


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Ponto de entrada da Lambda (acionado pelo EventBridge).

    Args:
        event: payload do evento (não usado; o agendamento não envia dados úteis).
        context: contexto de execução da Lambda (runtime info).

    Returns:
        Dicionário com o resumo da execução (útil para logs/observabilidade).

    Raises:
        Exception: relança qualquer falha de ingestão para que a Lambda seja
            marcada como erro (e dispare alarmes/retentativas se configurados).
    """
    bucket_bronze = _get_required_env("BRONZE_BUCKET")
    # SGS_SERIES vem como uma string separada por vírgula, ex.: "12,433".
    series_raw = _get_required_env("SGS_SERIES")
    series = [s.strip() for s in series_raw.split(",") if s.strip()]

    # GLUE_JOB_NAME é opcional. Se ausente, não dispara o job (útil em dev/testes).
    nome_glue_job = os.environ.get("GLUE_JOB_NAME")

    data_ingestao = dt.date.today()
    cliente_s3 = boto3.client("s3")

    resultados: list[dict[str, Any]] = []

    for codigo in series:
        try:
            registros = fetch_serie(codigo)
            chave = montar_chave_s3(codigo, data_ingestao)
            salvar_bronze(cliente_s3, bucket_bronze, chave, registros)
            resultados.append(
                {"serie": codigo, "registros": len(registros), "chave": chave}
            )
        except Exception:
            # Logamos qual série falhou e relançamos: preferimos falha explícita
            # a uma ingestão silenciosamente incompleta.
            logger.exception("Falha ao ingerir a série %s", codigo)
            raise

    # Ingestão concluída com sucesso. Dispara a transformação, se configurada.
    if nome_glue_job:
        cliente_glue = boto3.client("glue")
        _disparar_glue_job(cliente_glue, nome_glue_job)

    resumo = {
        "status": "ok",
        "data_ingestao": data_ingestao.isoformat(),
        "series_ingeridas": resultados,
        "glue_job_disparado": bool(nome_glue_job),
    }
    logger.info("Ingestão concluída: %s", resumo)
    return resumo
