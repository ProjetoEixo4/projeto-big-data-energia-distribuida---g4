"""
Extração — IBGE SIDRA, IPCA (tabela 1737, variável 2266: número-índice, dez/1993 = 100)

Usado para deflacionar a tarifa de energia (transform_tarifa.py): sem isso,
"a tarifa subiu" mistura aumento real com inflação geral.

Uso:
    python etl/extract_ipca.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import requests

URL = "https://apisidra.ibge.gov.br/values/t/1737/n1/all/v/2266/p/201912-202512"
RAW_DIR = Path("data/raw/ibge_ipca")


def extrair(out_dir: Path = RAW_DIR) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[extract_ipca] consultando {URL}")
    r = requests.get(URL, timeout=120)
    r.raise_for_status()
    dados = r.json()
    if len(dados) < 2:
        raise RuntimeError("resposta vazia da SIDRA para o IPCA")
    destino = out_dir / "ipca_sidra_1737.json"
    destino.write_text(json.dumps(dados, ensure_ascii=False), encoding="utf-8")
    print(f"[extract_ipca] {len(dados) - 1} meses salvos em {destino}")
    return destino


if __name__ == "__main__":
    try:
        extrair()
    except (requests.RequestException, RuntimeError) as e:
        print(f"[extract_ipca] ERRO: {e}", file=sys.stderr)
        sys.exit(1)
