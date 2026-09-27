"""
Extração — IBGE | SIDRA API (PIB municipal e população)
Fonte: https://sidra.ibge.gov.br/

Ao contrário das demais fontes, o IBGE é consultado via API REST (JSON),
não requer download de arquivo bruto. Consulta a tabela 5938 (PIB dos
Municípios) para o recorte 2020-2025 definido com o orientador.

RESSALVA IMPORTANTE: o PIB dos Municípios tem defasagem de divulgação de
aproximadamente 2 anos. A edição mais recente disponível (dez/2025) cobre
apenas até o ano de 2023 — os anos de 2024 e 2025 ficarão ausentes desta
fonte até que o IBGE publique novas edições. Essa lacuna é uma limitação
conhecida do projeto (ver Etapa 1, item 1.2), não um erro de extração.
"""
import datetime as dt
import json
import os

import requests

# Tabela 5938: PIB a preços correntes, dos Municípios.
# Períodos 2020 a 2023 (últimos disponíveis na divulgação de dez/2025);
# 2024 e 2025 devem ser reconsultados quando o IBGE publicar novas edições.
ANO_INICIO, ANO_FIM = 2020, 2025
SIDRA_URL = (
    "https://apisidra.ibge.gov.br/values/t/5938/n6/all/v/37/p/2020,2021,2022,2023"
)

RAW_DIR = os.path.join("data", "raw", "ibge_sidra")


def extract(url: str = SIDRA_URL) -> str:
    os.makedirs(RAW_DIR, exist_ok=True)
    today = dt.date.today().isoformat()
    out_path = os.path.join(RAW_DIR, f"ibge_pib_municipios_{today}.json")

    print(f"[extract_ibge_sidra] consultando {url} ...")
    print(
        f"[extract_ibge_sidra] AVISO: recorte do projeto é {ANO_INICIO}-{ANO_FIM}, "
        "mas o IBGE só publicou PIB municipal até 2023 até o momento."
    )
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    data = resp.json()

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    print(f"[extract_ibge_sidra] {len(data) - 1} registros salvos em {out_path}")
    return out_path


if __name__ == "__main__":
    extract()
