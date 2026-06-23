"""Glue Job de transformação (camada Bronze -> Silver) do DataLake BACEN.

Este script é executado como um AWS Glue Job do tipo **Python Shell** (não Spark).
Por quê Python Shell e não Spark? As séries do SGS são pequenas (centenas a
poucos milhares de pontos). Spark seria desperdício de recurso e complexidade.
"A tecnologia é consequência do problema, não o ponto de partida."

O que o job faz:

1. Lê todos os JSONs brutos da camada Bronze no S3.
2. Limpa e tipa os dados (datas e valores).
3. Escreve em Parquet particionado por ano/mês na camada Silver no S3.

Organização do código:

- As funções de transformação no topo do módulo são **puras** (só dependem de
  pandas). Não tocam em AWS. Isso as torna fáceis de testar com pytest, sem
  precisar de Spark, credenciais ou rede.
- Toda a parte específica de AWS/Glue (ler argumentos, S3, etc.) fica dentro de
  `main()` e do bloco `if __name__ == "__main__"`. Os imports do `awsglue` e
  `awswrangler` também ficam lá dentro — assim, importar este módulo nos testes
  NÃO exige essas bibliotecas (que só existem no ambiente do Glue).
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Funções de transformação PURAS (testáveis isoladamente, só dependem de pandas)
# ---------------------------------------------------------------------------


def parse_records(registros: list[dict[str, Any]], codigo: str) -> pd.DataFrame:
    """Converte a lista de registros brutos da API em um DataFrame tipado.

    A API do SGS retorna registros no formato:
        [{"data": "01/06/2026", "valor": "14.90"}, ...]

    Args:
        registros: lista de dicionários crus vinda do JSON da Bronze.
        codigo: código da série (vira a coluna `serie_codigo`).

    Returns:
        DataFrame com as colunas: data (datetime64), valor (float),
        serie_codigo (str). Linhas com data ou valor inválidos são removidas.
    """
    if not registros:
        # Retorna um DataFrame vazio com o schema esperado, em vez de quebrar.
        # Isso mantém o contrato da função estável para quem consome.
        return pd.DataFrame(
            {
                "data": pd.Series(dtype="datetime64[ns]"),
                "valor": pd.Series(dtype="float64"),
                "serie_codigo": pd.Series(dtype="object"),
            }
        )

    df = pd.DataFrame(registros)
    df = _converter_data(df)
    df = _converter_valor(df)
    df["serie_codigo"] = str(codigo)
    df = _remover_invalidos(df)
    return df.reset_index(drop=True)


def _converter_data(df: pd.DataFrame) -> pd.DataFrame:
    """Converte a coluna `data` (string "DD/MM/AAAA") para datetime64.

    Usa `errors="coerce"`: datas inválidas viram NaT em vez de estourar exceção.
    A remoção dos NaT acontece depois, em `_remover_invalidos`.
    """
    df = df.copy()
    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y", errors="coerce")
    return df


def _converter_valor(df: pd.DataFrame) -> pd.DataFrame:
    """Converte a coluna `valor` (string) para float.

    A API usa ponto como separador decimal (ex.: "14.90"), então
    `pd.to_numeric` resolve direto. `errors="coerce"` transforma valores
    inválidos em NaN.
    """
    df = df.copy()
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    return df


def _remover_invalidos(df: pd.DataFrame) -> pd.DataFrame:
    """Remove linhas com `data` (NaT) ou `valor` (NaN) inválidos.

    Limpeza típica da camada Silver: garantir que o dado tipado é confiável.
    """
    antes = len(df)
    df = df.dropna(subset=["data", "valor"])
    removidas = antes - len(df)
    if removidas:
        logger.warning("Removidas %d linhas inválidas (data/valor nulos)", removidas)
    return df


def adicionar_particoes(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona as colunas de partição `ano` e `mes` a partir da coluna `data`.

    Particionar por ano/mês no S3 deixa as consultas do Athena mais baratas e
    rápidas: o Athena lê só as partições necessárias (partition pruning), em vez
    de varrer o dataset inteiro.

    Args:
        df: DataFrame com a coluna `data` já em datetime64.

    Returns:
        DataFrame com as colunas extras `ano` (int) e `mes` (int).
    """
    df = df.copy()
    df["ano"] = df["data"].dt.year.astype("int64")
    df["mes"] = df["data"].dt.month.astype("int64")
    return df


def transformar(registros: list[dict[str, Any]], codigo: str) -> pd.DataFrame:
    """Pipeline de transformação completo Bronze -> Silver para uma série.

    Junta as etapas: parse/tipagem -> remoção de inválidos -> partições.
    Esta é a função pública principal, usada tanto pelo Glue Job quanto pelos
    testes unitários.

    Args:
        registros: registros brutos da Bronze.
        codigo: código da série.

    Returns:
        DataFrame Silver, limpo, tipado e com colunas de partição.
    """
    df = parse_records(registros, codigo)
    df = adicionar_particoes(df)
    return df


# ---------------------------------------------------------------------------
# Parte específica do AWS Glue (não é exercitada nos testes unitários)
# ---------------------------------------------------------------------------


def _ler_bronze(s3_path_bronze: str) -> dict[str, list[dict[str, Any]]]:
    """Lê todos os JSONs da Bronze e agrupa os registros por código de série.

    A chave do S3 da Bronze segue o padrão:
        .../serie_codigo=12/ingestion_date=.../dados.json

    Args:
        s3_path_bronze: prefixo S3 da camada Bronze (ex.: s3://bucket/sgs/).

    Returns:
        Dicionário {codigo_serie: [registros...]} agregando todas as ingestões.
    """
    import json
    import re

    import awswrangler as wr  # disponível no ambiente do Glue

    caminhos = wr.s3.list_objects(s3_path_bronze, suffix=".json")
    logger.info("Encontrados %d arquivos JSON na Bronze", len(caminhos))

    por_serie: dict[str, list[dict[str, Any]]] = {}
    for caminho in caminhos:
        # Extrai o código da série do próprio caminho (partição de origem).
        match = re.search(r"serie_codigo=([^/]+)/", caminho)
        codigo = match.group(1) if match else "desconhecido"

        bytes_json = _baixar_objeto(caminho)
        registros = json.loads(bytes_json.decode("utf-8"))
        por_serie.setdefault(codigo, []).extend(registros)

    return por_serie


def _baixar_objeto(s3_uri: str) -> bytes:
    """Baixa o conteúdo bruto de um objeto S3 dado seu URI (s3://bucket/key)."""
    import boto3

    sem_prefixo = s3_uri.replace("s3://", "", 1)
    bucket, _, chave = sem_prefixo.partition("/")
    cliente = boto3.client("s3")
    resposta = cliente.get_object(Bucket=bucket, Key=chave)
    return resposta["Body"].read()


def _escrever_silver(df: pd.DataFrame, s3_path_silver: str) -> None:
    """Escreve o DataFrame Silver em Parquet particionado por ano/mês no S3.

    Usa `awswrangler` com `dataset=True` e `mode="overwrite_partitions"`:
    reprocessar uma partição substitui só ela, sem duplicar dados.
    """
    import awswrangler as wr

    if df.empty:
        logger.warning("DataFrame Silver vazio — nada a escrever em %s", s3_path_silver)
        return

    resultado = wr.s3.to_parquet(
        df=df,
        path=s3_path_silver,
        dataset=True,
        mode="overwrite_partitions",
        partition_cols=["serie_codigo", "ano", "mes"],
    )
    logger.info(
        "Escritos %d arquivos Parquet na Silver", len(resultado.get("paths", []))
    )


def main() -> None:
    """Orquestra a execução do Glue Job: lê Bronze, transforma, escreve Silver."""
    import sys

    from awsglue.utils import getResolvedOptions  # só existe no ambiente do Glue

    args = getResolvedOptions(sys.argv, ["BRONZE_PATH", "SILVER_PATH"])
    bronze_path = args["BRONZE_PATH"]
    silver_path = args["SILVER_PATH"]

    logger.info("Iniciando transformação. Bronze=%s Silver=%s", bronze_path, silver_path)

    por_serie = _ler_bronze(bronze_path)

    frames: list[pd.DataFrame] = []
    for codigo, registros in por_serie.items():
        logger.info("Transformando série %s (%d registros)", codigo, len(registros))
        frames.append(transformar(registros, codigo))

    if not frames:
        logger.warning("Nenhum dado encontrado na Bronze. Encerrando.")
        return

    df_silver = pd.concat(frames, ignore_index=True)
    _escrever_silver(df_silver, silver_path)
    logger.info("Transformação concluída com sucesso.")


if __name__ == "__main__":
    main()
