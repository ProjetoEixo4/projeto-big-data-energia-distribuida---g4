"""
Camada de E/S compartilhada entre execução local e AWS Glue.

Todos os scripts de transformação leem a camada RAW e gravam a camada
TRUSTED em disco, relativos a OET_DATA_DIR (padrão: ./data). Localmente isso
é a pasta data/ do repositório; no Glue, o runner (glue/runner.py) aponta
OET_DATA_DIR para /tmp/oet e sincroniza com o S3 antes e depois do job:

    s3://<bucket-raw>/<fonte>/...      <-->  $OET_DATA_DIR/raw/<fonte>/...
    s3://<bucket-trusted>/<tabela>/... <-->  $OET_DATA_DIR/trusted/<tabela>.parquet

Assim o MESMO código roda no notebook do aluno e no Glue — o que se testa
localmente é exatamente o que vai para a nuvem.

A camada TRUSTED é gravada em Parquet (colunar, comprimido, com esquema
embutido) e não mais em CSV: preserva tipos (CNPJ como texto, datas, booleanos)
e é o formato que o Athena/Spark leem sem conversão.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd


def data_dir() -> Path:
    return Path(os.environ.get("OET_DATA_DIR", "data"))


def raw_dir(fonte: str = "") -> Path:
    return data_dir() / "raw" / fonte if fonte else data_dir() / "raw"


def trusted_dir() -> Path:
    d = data_dir() / "trusted"
    d.mkdir(parents=True, exist_ok=True)
    return d


def salvar_trusted(df: pd.DataFrame, nome: str) -> Path:
    caminho = trusted_dir() / f"{nome}.parquet"
    df.to_parquet(caminho, index=False, engine="pyarrow", compression="snappy")
    return caminho


def ler_trusted(nome: str) -> pd.DataFrame:
    """Lê a tabela trusted, seja arquivo único (pandas) ou pasta de partes (Spark/S3)."""
    pasta, arquivo = trusted_dir() / nome, trusted_dir() / f"{nome}.parquet"
    if pasta.is_dir():
        return pd.read_parquet(pasta, engine="pyarrow")
    if arquivo.exists():
        return pd.read_parquet(arquivo, engine="pyarrow")
    raise FileNotFoundError(f"{arquivo} não existe — rode antes o job/script que gera '{nome}'.")


def salvar_relatorio(nome: str, dados: dict) -> Path:
    """Relatório de qualidade (JSON) ao lado da camada trusted — evidência de execução."""
    caminho = trusted_dir() / f"_qualidade_{nome}.json"
    caminho.write_text(json.dumps(dados, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return caminho
