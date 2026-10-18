"""
Extração — ANEEL GD, informações técnicas das usinas fotovoltaicas

O arquivo principal de GD (extract_aneel_gd.py) é um cadastro SEM data de
conexão. Esta tabela complementar traz, por usina fotovoltaica,
CodGeracaoDistribuida (mesmo código de CodEmpreendimento do cadastro) e
DatConexao — a data em que a usina entrou em operação. É ela que permite
reconstruir a série mensal de capacidade instalada 2020-2025 (ver
transform_geracao_uf.py).

Uso:
    python etl/extract_aneel_gd_tecnico.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _download import baixar  # noqa: E402

URL = (
    "https://dadosabertos.aneel.gov.br/dataset/5e0fafd2-21b9-4d5b-b622-40438d40aba2/"
    "resource/703c4cb8-b7e2-4f27-a9bb-7e55324a88a4/download/"
    "empreendimento-gd-informacoes-tecnicas-fotovoltaica.parquet"
)
RAW_DIR = Path("data/raw/aneel_gd_tecnico")


def extrair(out_dir: Path = RAW_DIR) -> Path:
    return baixar(URL, out_dir / "empreendimento-gd-informacoes-tecnicas-fotovoltaica.parquet",
                  "extract_aneel_gd_tecnico")


if __name__ == "__main__":
    try:
        extrair()
    except requests.RequestException as e:
        print(f"[extract_aneel_gd_tecnico] ERRO: {e}", file=sys.stderr)
        sys.exit(1)
