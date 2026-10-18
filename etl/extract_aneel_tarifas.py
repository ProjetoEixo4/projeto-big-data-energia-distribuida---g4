"""
Extração — ANEEL Tarifas de Energia Elétrica
Baixa o CSV de tarifas homologadas por distribuidora, disponível no portal
de dados abertos da ANEEL.

Esse arquivo é o histórico completo de tarifas de todas as distribuidoras
do Brasil e pode ser grande (dezenas a centenas de MB, dependendo da
atualização da base). O download é feito em streaming, com progresso
impresso a cada alguns MB, para deixar claro que o processo está
avançando e não travado. Pode levar alguns minutos dependendo da sua
conexão — deixe rodar até o fim.

Uso:
    python etl/extract_aneel_tarifas.py
"""
import sys
from pathlib import Path

import requests

URL_ANEEL_TARIFAS_CSV = (
    "https://dadosabertos.aneel.gov.br/dataset/5a583f3e-1646-4f67-bf0f-"
    "69db4203e89e/resource/fcf2906c-7c32-4b9b-a637-054e7a5234f4/download/"
    "tarifas-homologadas-distribuidoras-energia-eletrica.csv"
)

RAW_DIR = Path("data/raw/aneel_tarifas")
CHUNK_SIZE = 1024 * 1024  # 1 MB por chunk


def download_csv(out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "tarifas-homologadas-distribuidoras-energia-eletrica.csv"
    tmp_file = out_file.with_suffix(".csv.part")

    print(f"[extract_aneel_tarifas] baixando de: {URL_ANEEL_TARIFAS_CSV}")

    with requests.get(URL_ANEEL_TARIFAS_CSV, stream=True, timeout=(15, 120)) as resp:
        resp.raise_for_status()
        total_bytes = int(resp.headers.get("Content-Length", 0))
        total_mb = total_bytes / (1024 * 1024) if total_bytes else None

        baixado = 0
        proximo_log_mb = 10
        with open(tmp_file, "wb") as f:
            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                if not chunk:
                    continue
                f.write(chunk)
                baixado += len(chunk)
                baixado_mb = baixado / (1024 * 1024)
                if baixado_mb >= proximo_log_mb:
                    if total_mb:
                        print(f"[extract_aneel_tarifas] {baixado_mb:,.0f} MB / {total_mb:,.0f} MB baixados...")
                    else:
                        print(f"[extract_aneel_tarifas] {baixado_mb:,.0f} MB baixados...")
                    proximo_log_mb += 10

    tmp_file.replace(out_file)

    # Conta linhas sem carregar tudo na memória de uma vez
    n_linhas = 0
    with open(out_file, "r", encoding="latin1", errors="ignore") as f:
        for _ in f:
            n_linhas += 1
    print(f"[extract_aneel_tarifas] concluído: {n_linhas:,} linhas salvas em {out_file}")
    return out_file


if __name__ == "__main__":
    try:
        download_csv(RAW_DIR)
    except requests.RequestException as e:
        print(f"[extract_aneel_tarifas] ERRO ao baixar: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n[extract_aneel_tarifas] interrompido pelo usuário — rode novamente para retomar do início.", file=sys.stderr)
        sys.exit(1)
