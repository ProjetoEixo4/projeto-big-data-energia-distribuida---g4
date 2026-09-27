"""
Extração — INMET Banco de Dados Meteorológicos (BDMEP)
Baixa os arquivos ZIP anuais do portal de dados históricos do INMET.
Cada ZIP contém um CSV por estação automática (500+ arquivos por ano).

Uso:
    python etl/extract_inmet.py --ano-inicio 2020 --ano-fim 2025

Nota: o download é feito ANO A ANO, sem filtro de estação — a seletividade
(agregação por UF) acontece na fase de transformação, para preservar o
dado bruto integralmente na camada raw.

Cada ano é baixado com progresso em MB e gravado primeiro num arquivo
temporário (.part); só é renomeado para o nome final se o download
terminar completo. Assim, se você interromper no meio, o script não
acha erroneamente que aquele ano já está pronto na próxima execução.
"""
import argparse
import sys
from pathlib import Path

import requests

URL_TEMPLATE = "https://portal.inmet.gov.br/uploads/dadoshistoricos/{ano}.zip"
RAW_DIR = Path("data/raw/inmet")
CHUNK_SIZE = 1024 * 1024  # 1 MB


def download_ano(ano: int, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{ano}.zip"
    tmp_file = out_dir / f"{ano}.zip.part"

    if out_file.exists():
        print(f"[extract_inmet] {ano}.zip já existe ({out_file.stat().st_size/1e6:.1f} MB) — pulando")
        return out_file

    url = URL_TEMPLATE.format(ano=ano)
    print(f"[extract_inmet] baixando {ano}: {url}")
    with requests.get(url, stream=True, timeout=(15, 180)) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        baixado = 0
        proximo_log_mb = 5
        with open(tmp_file, "wb") as f:
            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                if not chunk:
                    continue
                f.write(chunk)
                baixado += len(chunk)
                baixado_mb = baixado / 1e6
                if total:
                    pct = 100 * baixado / total
                    print(f"\r[extract_inmet]   {ano}: {pct:5.1f}% ({baixado_mb:,.0f} MB / {total/1e6:,.0f} MB)", end="", flush=True)
                elif baixado_mb >= proximo_log_mb:
                    print(f"[extract_inmet]   {ano}: {baixado_mb:,.0f} MB baixados...")
                    proximo_log_mb += 5
    print()
    tmp_file.replace(out_file)
    print(f"[extract_inmet] {ano}.zip salvo em {out_file} ({out_file.stat().st_size/1e6:.1f} MB)")
    return out_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extração INMET — BDMEP (dados históricos anuais)")
    parser.add_argument("--ano-inicio", type=int, default=2020)
    parser.add_argument("--ano-fim", type=int, default=2025)
    parser.add_argument("--out-dir", default=str(RAW_DIR))
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    erros = []
    for ano in range(args.ano_inicio, args.ano_fim + 1):
        try:
            download_ano(ano, out_dir)
        except requests.RequestException as e:
            print(f"[extract_inmet] ERRO no ano {ano}: {e}", file=sys.stderr)
            erros.append(ano)
        except KeyboardInterrupt:
            print(f"\n[extract_inmet] interrompido pelo usuário no ano {ano} — rode de novo, os anos já concluídos serão pulados.", file=sys.stderr)
            sys.exit(1)

    if erros:
        print(f"[extract_inmet] concluído com falhas nos anos: {erros}", file=sys.stderr)
        sys.exit(1)
    print(f"[extract_inmet] concluído: {args.ano_inicio}-{args.ano_fim} baixados em {out_dir}")
