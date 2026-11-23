"""Glue Job de transformação (camada Bronze -> Silver) do DataLake BACEN

Este script é executado como um AWS Glue Job do tipo **Python Shell** (não Spark)
Porque as séries do SGS são pequenas, Spark seria desperdício de recurso e complexidade

O que o job faz:

1. Lê todos os JSONs brutos da camada Bronze no S3
2. Limpa e tipa os dados (datas e valores)
3. Escreve em Parquet particionado por ano/mês na camada Silver no S3

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
    
    df = df.copy()
    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y", errors="coerce")
    return df


def _converter_valor(df: pd.DataFrame) -> pd.DataFrame:
    
    df = df.copy()
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    return df


def _remover_invalidos(df: pd.DataFrame) -> pd.DataFrame:

    antes = len(df)
    df = df.dropna(subset=["data", "valor"])
    removidas = antes - len(df)
    if removidas:
        logger.warning("Removidas %d linhas inválidas (data/valor nulos)", removidas)
    return df


def adicionar_particoes(df: pd.DataFrame) -> pd.DataFrame:
    
    df = df.copy()
    df["ano"] = df["data"].dt.year.astype("int64")
    df["mes"] = df["data"].dt.month.astype("int64")
    return df


def manter_ingestao_recente(df: pd.DataFrame) -> pd.DataFrame:
        """Mantém, para cada data, só o registro da ingestão mais recente"""

        if "ingestion_date" in df.columns:
            df = df.sort_values(by="ingestion_date").drop_duplicates(subset=["data"], keep="last")

        return df


def carimbar_ingestion_date(registros, data_ingestao):
    resultado = []
    for registro in registros:
        novo = registro.copy()
        novo["ingestion_date"] = data_ingestao
        resultado.append(novo)
    return resultado


def transformar(registros: list[dict[str, Any]], codigo: str) -> pd.DataFrame:
    
    df = parse_records(registros, codigo)
    df = manter_ingestao_recente(df)
    df = adicionar_particoes(df)
    return df


# ---------------------------------------------------------------------------
# Parte específica do AWS Glue (não é exercitada nos testes unitários)
# ---------------------------------------------------------------------------

def extrair_ingestion_date(caminho: str) -> str:
    import re

    match = re.search(r"ingestion_date=([^/]+)/", caminho)
    if match is None:
        raise ValueError(f"ingestion_date não encontrado no caminho: {caminho}")
    
    return match.group(1)


def _ler_bronze(s3_path_bronze: str) -> dict[str, list[dict[str, Any]]]:
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

        data_ingestao = extrair_ingestion_date(caminho)

        bytes_json = _baixar_objeto(caminho)
        registros = json.loads(bytes_json.decode("utf-8"))

        carimbados = carimbar_ingestion_date(registros, data_ingestao)
        por_serie.setdefault(codigo, []).extend(carimbados)
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
