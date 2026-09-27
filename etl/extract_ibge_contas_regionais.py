"""
Extração — IBGE PIB por UF
Consulta a API SIDRA, tabela 5938 (Produto Interno Bruto a preços correntes),
no nível geográfico n3 = Unidade da Federação.

Essa é a MESMA tabela usada anteriormente para o PIB dos Municípios (nível
n6), só que agora consultada no nível estadual (n3), com a variável 37
(PIB a preços correntes). A tabela 6784 (Contas Regionais) foi descartada
porque a API retorna 400 Bad Request para a combinação de parâmetros que
o projeto precisa — a 5938 cobre a mesma necessidade sem esse problema.

Uso:
    python etl/extract_ibge_contas_regionais.py --anos 2020 2021 2022 2023
"""
import argparse
import json
import sys
from pathlib import Path

import requests

TABELA = "5938"
VARIAVEL = "37"  # Produto Interno Bruto a preços correntes (R$ mil)
NIVEL_GEOGRAFICO = "n3"  # n3 = Unidade da Federação (n6 seria município)
RAW_DIR = Path("data/raw/ibge_contas_regionais")


def montar_url(anos: list[str]) -> str:
    periodos = ",".join(anos)
    return f"https://apisidra.ibge.gov.br/values/t/{TABELA}/{NIVEL_GEOGRAFICO}/all/v/{VARIAVEL}/p/{periodos}"


def extrair(anos: list[str], out_dir: Path) -> Path:
    url = montar_url(anos)
    print(f"[extract_ibge_contas_regionais] consultando: {url}")

    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    dados = resp.json()

    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"ibge_pib_uf_{'_'.join(anos)}.json"
    out_file.write_text(json.dumps(dados, ensure_ascii=False, indent=2), encoding="utf-8")

    # dados[0] é o cabeçalho da SIDRA, não um registro
    n_registros = len(dados) - 1
    print(f"[extract_ibge_contas_regionais] {n_registros:,} registros salvos em {out_file}")
    return out_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extração IBGE — PIB por UF (tabela 5938, nível estadual)")
    parser.add_argument("--anos", nargs="+", default=["2020", "2021", "2022", "2023"])
    parser.add_argument("--out-dir", default=str(RAW_DIR))
    args = parser.parse_args()

    try:
        extrair(args.anos, Path(args.out_dir))
    except requests.RequestException as e:
        print(f"[extract_ibge_contas_regionais] ERRO ao consultar API: {e}", file=sys.stderr)
        sys.exit(1)
