"""
Extração — EPE Balanço Energético Nacional (BEN), "matriz em unidades comerciais"

O BEN não tem URL de download estável processo foi MANUAL e NÃO baixa da internet. O script
apenas organiza os arquivos já baixados na estrutura padrão da camada raw.


Uso (importamos todos os anos de uma pasta de uma vez):
    python etl/extract_epe_ben.py --pasta "C:/Users/lucas/Downloads/projeto Eixo 4/EPE"
Ou um arquivo só:
    python etl/extract_epe_ben.py --arquivo caminho/arquivo.xlsx --ano-base 2025

Para a nuvem: depois de organizar, enviamos data/raw/epe_ben/ ao bucket RAW
(aws s3 sync data/raw/epe_ben s3://projeto-eixo4-g3-raw/epe_ben) — o pipeline lê de lá.
"""
from __future__ import annotations

import argparse
import re
import shutil
import sys
from pathlib import Path

RAW_DIR = Path("data/raw/epe_ben")
PADRAO = re.compile(r"matriz_balanco_energitico_unidades_comerciais_(\d{4})\.xlsx$", re.IGNORECASE)


def importar_pasta(pasta: Path, out_dir: Path) -> list[Path]:
    achados = sorted((int(m.group(1)), p) for p in pasta.iterdir() if (m := PADRAO.search(p.name)))
    if not achados:
        raise FileNotFoundError(f"nenhum 'matriz_balanco_energitico_unidades_comerciais_AAAA.xlsx' em {pasta}")
    out_dir.mkdir(parents=True, exist_ok=True)
    saidas = []
    for ano, origem in achados:
        destino = out_dir / f"ben_comerciais_{ano}.xlsx"
        shutil.copy2(origem, destino)
        print(f"[extract_epe_ben] {origem.name} -> {destino}")
        saidas.append(destino)
    return saidas


def importar_arquivo(origem: Path, ano: str, out_dir: Path) -> Path:
    if not origem.exists():
        raise FileNotFoundError(f"arquivo não encontrado: {origem}")
    out_dir.mkdir(parents=True, exist_ok=True)
    destino = out_dir / f"ben_comerciais_{ano}{origem.suffix}"
    shutil.copy2(origem, destino)
    print(f"[extract_epe_ben] {origem.name} -> {destino}")
    return destino


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Organização do BEN (EPE) — arquivos baixados manualmente")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--pasta", help="pasta com os arquivos matriz_balanco_energitico_unidades_comerciais_AAAA.xlsx")
    g.add_argument("--arquivo", help="um arquivo isolado (use com --ano-base)")
    ap.add_argument("--ano-base", help="ano de referência do arquivo isolado")
    ap.add_argument("--out-dir", default=str(RAW_DIR))
    a = ap.parse_args()
    try:
        if a.pasta:
            importar_pasta(Path(a.pasta).expanduser(), Path(a.out_dir))
        else:
            if not a.ano_base:
                ap.error("--arquivo exige --ano-base")
            importar_arquivo(Path(a.arquivo).expanduser(), a.ano_base, Path(a.out_dir))
    except FileNotFoundError as e:
        print(f"[extract_epe_ben] ERRO: {e}", file=sys.stderr)
        sys.exit(1)
