"""Testes de integração do fluxo de ingestão (lambda/handler.py).

Aqui validamos o fluxo Lambda -> S3 Bronze de ponta a ponta, mas SEM tocar em
serviços reais:

  - A API do BACEN é simulada com mock em `urllib.request.urlopen`.
  - O S3 é simulado com `moto` (S3 em memória), então `boto3` funciona normalmente
    sem precisar de credenciais ou rede.

Assim o teste é rápido, determinístico e não depende de internet — exatamente o
que se espera de um bom teste automatizado.
"""

import io
import json
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

import handler


BUCKET_BRONZE = "datalake-bacen-dev-bronze-teste"


@pytest.fixture
def ambiente(monkeypatch):
    """Configura as variáveis de ambiente que a Lambda lê.

    Não define GLUE_JOB_NAME de propósito: assim o handler NÃO tenta disparar o
    Glue Job, mantendo o teste focado só na ingestão Bronze.
    """
    monkeypatch.setenv("BRONZE_BUCKET", BUCKET_BRONZE)
    monkeypatch.setenv("SGS_SERIES", "12,433")
    monkeypatch.delenv("GLUE_JOB_NAME", raising=False)


def _resposta_fake(payload: list[dict]):
    """Cria um objeto que imita a resposta de urlopen (context manager + read)."""
    corpo = json.dumps(payload).encode("utf-8")

    class RespostaFake(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    return RespostaFake(corpo)


@mock_aws
def test_ingestao_salva_bronze_no_s3(ambiente):
    """O handler deve buscar cada série e salvar um JSON na Bronze por série."""
    # Cria o bucket Bronze no S3 simulado.
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET_BRONZE)

    payload = [
        {"data": "01/01/2026", "valor": "11.25"},
        {"data": "01/02/2026", "valor": "11.50"},
    ]

    # Mocka a chamada HTTP. Usamos side_effect (não return_value) para devolver
    # uma resposta NOVA a cada chamada: um BytesIO só pode ser lido uma vez, e o
    # handler chama urlopen uma vez por série.
    with patch(
        "handler.urllib.request.urlopen",
        side_effect=lambda *a, **k: _resposta_fake(payload),
    ):
        resultado = handler.lambda_handler({}, None)

    # O resumo deve indicar sucesso e duas séries ingeridas.
    assert resultado["status"] == "ok"
    assert len(resultado["series_ingeridas"]) == 2
    assert resultado["glue_job_disparado"] is False

    # Confirma que dois objetos foram realmente gravados na Bronze.
    objetos = s3.list_objects_v2(Bucket=BUCKET_BRONZE)
    assert objetos["KeyCount"] == 2

    # Confirma que o conteúdo gravado bate com o payload da API.
    chave = objetos["Contents"][0]["Key"]
    corpo = s3.get_object(Bucket=BUCKET_BRONZE, Key=chave)["Body"].read()
    assert json.loads(corpo) == payload


@mock_aws
def test_ingestao_propaga_erro_da_api(ambiente):
    """Se a API falhar, o handler deve relançar a exceção (falha explícita)."""
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET_BRONZE)

    with patch("handler.urllib.request.urlopen", side_effect=OSError("rede caiu")):
        with pytest.raises(OSError):
            handler.lambda_handler({}, None)


def test_montar_chave_s3_formato():
    """A chave da Bronze deve seguir o padrão particionado esperado."""
    import datetime as dt

    chave = handler.montar_chave_s3("12", dt.date(2026, 6, 23))
    assert chave == "sgs/serie_codigo=12/ingestion_date=2026-06-23/dados.json"
