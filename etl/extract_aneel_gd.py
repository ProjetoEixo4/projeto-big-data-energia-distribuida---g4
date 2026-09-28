"""
Extração — ANEEL Geração Distribuída (GD)


Uso:
    python etl/extract_aneel_gd.py
    python etl/extract_aneel_gd.py --uf MG

O filtro --uf é opcional e serve apenas para reduzir o arquivo localmente
durante testes. O filtro temporal (2020-2025) é aplicado na FASE DE
TRANSFORMAÇÃO, não aqui — a camada raw deve preservar o dado como veio
da fonte.
"""
import argparse
import sys
from pathlib import Path

import requests

URL_ANEEL_GD_PARQUET = (
    "https://dadosabertos.aneel.gov.br/dataset/5e0fafd2-21b9-4d5b-b622-"
    "40438d40aba2/resource/cd29f6eb-e08d-4db7-b6fb-ed6e3b682d27/download/"
    "empreendimento-geracao-distribuida.parquet"
)

RAW_DIR = Path("data/raw/aneel_gd")
CHUNK_SIZE = 1024 * 1024  # 1 MB


def download_parquet(out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "empreendimento-geracao-distribuida.parquet"
    tmp_file = out_file.with_suffix(".parquet.part")

    print(f"[extract_aneel_gd] baixando de: {URL_ANEEL_GD_PARQUET}")
    # timeout=(connect, read): dá mais tolerância para pacotes lentos no meio do arquivo
    with requests.get(URL_ANEEL_GD_PARQUET, stream=True, timeout=(15, 300)) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        baixado = 0
        proximo_log_mb = 20
        with open(tmp_file, "wb") as f:
            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                if not chunk:
                    continue
                f.write(chunk)
                baixado += len(chunk)
                baixado_mb = baixado / 1e6
                if total:
                    pct = 100 * baixado / total
                    print(f"\r[extract_aneel_gd] {pct:5.1f}% ({baixado_mb:,.0f} MB / {total/1e6:,.0f} MB)", end="", flush=True)
                elif baixado_mb >= proximo_log_mb:
                    print(f"[extract_aneel_gd] {baixado_mb:,.0f} MB baixados...")
                    proximo_log_mb += 20
    print()
    tmp_file.replace(out_file)
    print(f"[extract_aneel_gd] arquivo salvo em {out_file} ({out_file.stat().st_size/1e6:.1f} MB)")
    return out_file


def filtrar_por_uf(parquet_path: Path, uf: str) -> None:
    """Filtro opcional só para inspeção local rápida — não é usado no pipeline em produção."""
    import pandas as pd

    df = pd.read_parquet(parquet_path)
    col_uf = next((c for c in df.columns if "UF" in c.upper() or "SIG" in c.upper()), None)
    if col_uf is None:
        print("[extract_aneel_gd] aviso: coluna de UF não identificada automaticamente; "
              "confira os nomes de coluna do arquivo (dicionário de dados no portal).")
        return
    filtrado = df[df[col_uf].str.upper() == uf.upper()]
    out_file = parquet_path.parent / f"empreendimento-geracao-distribuida_{uf.upper()}.parquet"
    filtrado.to_parquet(out_file, index=False)
    print(f"[extract_aneel_gd] {len(filtrado):,} registros de {uf.upper()} salvos em {out_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extração ANEEL — Geração Distribuída")
    parser.add_argument("--uf", help="Sigla da UF para gerar um recorte local de teste (opcional)")
    parser.add_argument("--out-dir", default=str(RAW_DIR), help="Pasta de destino")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    try:
        arquivo = download_parquet(out_dir)
    except requests.RequestException as e:
        print(f"[extract_aneel_gd] ERRO ao baixar: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n[extract_aneel_gd] interrompido pelo usuário — rode novamente para retomar do início.", file=sys.stderr)
        sys.exit(1)

    if args.uf:
        filtrar_por_uf(arquivo, args.uf)
