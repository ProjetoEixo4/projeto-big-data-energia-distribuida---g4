"""
Extração — EPE Balanço Energético Nacional (BEN)

IMPORTANTE: diferente das demais fontes, o BEN não tem uma URL de download
direto e estável — os dados tabulares (formato tidyverse) ficam dentro de
um dashboard interativo (https://dashboard.epe.gov.br/apps/livro-ben/),
onde é preciso escolher o ano-base e clicar em "download" manualmente.

Este script NÃO baixa da internet. Ele organiza um arquivo que você já
baixou manualmente do dashboard, copiando-o para a estrutura padrão da
camada raw — assim o restante do pipeline (transformação, carga) trata
essa fonte exatamente como as outras.

Uso:
    1. Acesse https://dashboard.epe.gov.br/apps/livro-ben/#anexo
    2. Escolha o ano-base desejado (ex.: 2025, que traz dados de 2024)
    3. Baixe o arquivo (xlsx ou csv) para a pasta Downloads
    4. Rode:
       python etl/extract_epe_ben.py --arquivo ~/Downloads/ben_anexo_2025.xlsx --ano-base 2025
"""
import argparse
import shutil
import sys
from pathlib import Path

RAW_DIR = Path("data/raw/epe_ben")


def organizar_arquivo_manual(arquivo_origem: Path, ano_base: str, out_dir: Path) -> Path:
    if not arquivo_origem.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {arquivo_origem}\n"
            "Baixe primeiro pelo dashboard: https://dashboard.epe.gov.br/apps/livro-ben/#anexo"
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    extensao = arquivo_origem.suffix
    out_file = out_dir / f"ben_{ano_base}{extensao}"
    shutil.copy2(arquivo_origem, out_file)

    print(f"[extract_epe_ben] arquivo do ano-base {ano_base} organizado em {out_file}")
    return out_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Organização do BEN (EPE) — download manual")
    parser.add_argument("--arquivo", required=True, help="Caminho do arquivo baixado manualmente do dashboard")
    parser.add_argument("--ano-base", required=True, help="Ano-base dos dados (ex.: 2025 para o BEN 2026)")
    parser.add_argument("--out-dir", default=str(RAW_DIR))
    args = parser.parse_args()

    try:
        organizar_arquivo_manual(Path(args.arquivo).expanduser(), args.ano_base, Path(args.out_dir))
    except FileNotFoundError as e:
        print(f"[extract_epe_ben] ERRO: {e}", file=sys.stderr)
        sys.exit(1)
