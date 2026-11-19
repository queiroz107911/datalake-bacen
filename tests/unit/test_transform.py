"""Testes unitários das funções de transformação (glue/transform_job.py).

Testam SÓ as funções puras (pandas). Não tocam em AWS, rede ou Spark — rodam
rápido e de forma determinística. É exatamente para isso que separamos a lógica
de transformação da parte de I/O da nuvem.
"""

import pandas as pd
import pytest

import transform_job as tj


@pytest.fixture
def registros_validos() -> list[dict]:
    """Amostra de registros no formato cru da API do SGS."""
    return [
        {"data": "01/01/2026", "valor": "11.25"},
        {"data": "01/02/2026", "valor": "11.50"},
        {"data": "15/03/2026", "valor": "12.00"}
    ]


def test_parse_records_tipa_colunas(registros_validos):
    """parse_records deve tipar data (datetime) e valor (float) corretamente."""
    df = tj.parse_records(registros_validos, codigo="12")

    assert list(df.columns) == ["data", "valor", "serie_codigo"]
    assert pd.api.types.is_datetime64_any_dtype(df["data"])
    assert pd.api.types.is_float_dtype(df["valor"])
    assert (df["serie_codigo"] == "12").all()
    assert len(df) == 3


def test_parse_records_converte_valores(registros_validos):
    """Os valores devem virar float com o número correto."""
    df = tj.parse_records(registros_validos, codigo="12")
    assert df["valor"].tolist() == [11.25, 11.50, 12.00]


def test_parse_records_remove_invalidos():
    """Linhas com data ou valor inválidos devem ser descartadas."""
    registros = [
        {"data": "01/01/2026", "valor": "10.0"},
        {"data": "data-invalida", "valor": "5.0"},   # data inválida -> NaT
        {"data": "03/01/2026", "valor": "abc"},      # valor inválido -> NaN
        {"data": "04/01/2026", "valor": "7.5"},
    ]
    df = tj.parse_records(registros, codigo="433")
    assert len(df) == 2
    assert df["valor"].tolist() == [10.0, 7.5]


def test_parse_records_lista_vazia_retorna_schema_estavel():
    """Com lista vazia, retorna DataFrame vazio mas com as colunas esperadas."""
    df = tj.parse_records([], codigo="12")
    assert df.empty
    assert list(df.columns) == ["data", "valor", "serie_codigo"]


def test_adicionar_particoes():
    """adicionar_particoes deve extrair ano e mes corretos da coluna data."""
    df = pd.DataFrame(
        {
            "data": pd.to_datetime(["2026-01-15", "2024-12-31"]),
            "valor": [1.0, 2.0],
            "serie_codigo": ["12", "12"],
        }
    )
    resultado = tj.adicionar_particoes(df)
    assert resultado["ano"].tolist() == [2026, 2024]
    assert resultado["mes"].tolist() == [1, 12]


def test_transformar_pipeline_completo(registros_validos):
    """transformar deve devolver o DataFrame Silver completo com partições."""
    df = tj.transformar(registros_validos, codigo="12")
    esperado = {"data", "valor", "serie_codigo", "ano", "mes"}
    assert esperado.issubset(set(df.columns))
    assert len(df) == 3
    assert df["ano"].tolist() == [2026, 2026, 2026]
    assert df["mes"].tolist() == [1, 2, 3]


def test_mantem_ingestao_recente():
    """Cada data deve aparecer uma vez, com o valor da ingestão mais recente"""
    registros = [
        {"data": "01/01/2026", "valor": "10.00", "ingestion_date": "2026-01-03"},
        {"data": "02/01/2026", "valor": "11.00", "ingestion_date": "2026-01-04"},
        {"data": "03/01/2026", "valor": "12.00", "ingestion_date": "2026-01-05"},
        {"data": "04/01/2026", "valor": "13.00", "ingestion_date": "2026-01-06"},
        {"data": "03/01/2026", "valor": "12.50", "ingestion_date": "2026-01-07"},
    ]
    df = tj.transformar(registros, codigo="433")
    
    assert len(df) == 4
    
    valor_dia_3 = df[df["data"] == "2026-01-03"]["valor"].iloc[0] 
    assert valor_dia_3 == 12.50  # O valor mais recente para a data 03/01/2026 deve ser mantido
    

def test_ingestion_date():

    caminho = "s3://bucket/sgs/serie_codigo=12/ingestion_date=2026-06-23/dados.json"

    resultado = tj.extrair_ingestion_date(caminho)
    assert resultado == "2026-06-23"


def test_extrair_ingestion_date_sem_data_gera_erro():
    caminho = "s3://bucket/sgs/serie_codigo=12/dados.json"   # sem ingestion_date

    with pytest.raises(ValueError):
        tj.extrair_ingestion_date(caminho)
