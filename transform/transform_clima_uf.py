"""
Transformação — INMET (clima) → dim_clima_uf

Cada ZIP anual do INMET tem 500+ CSVs, um por estação automática — inviável
manter no modelo por estação (ver justificativa completa na Etapa 1/2 do
documento: adota-se amostragem/agregação estatística por UF, análoga ao que
a climatologia faz ao agregar estações de uma rede regional).

Pipeline, por ano e por estação:
    1. Lê o CSV de dentro do ZIP (pula as 8 linhas de metadado do cabeçalho).
    2. A sigla da UF vem do NOME DO ARQUIVO (INMET_<REGIAO>_<UF>_...), que é
       mais confiável que reparsear a segunda linha do cabeçalho.
    3. Agrega HORA -> MÊS por estação: média de radiação, média de
       temperatura, soma de precipitação.
    4. Agrega ESTAÇÃO -> UF: média das médias/somas das estações daquele
       UF-mês (representa o "estado médio" do clima na UF naquele mês) e
       conta quantas estações contribuíram (qtd_estacoes). amostra_reduzida
       fica True quando menos de 3 estações contribuíram no mês — sinaliza
       um UF-mês com pouca cobertura para quem for interpretar os dados.

Uso:
    python transform/transform_clima_uf.py
Gera:
    data/trusted/dim_clima_uf.csv
"""
import re
import sys
import zipfile
from io import BytesIO
from pathlib import Path

import pandas as pd

RAW_DIR = Path("data/raw/inmet")
TRUSTED_DIR = Path("data/trusted")

ANO_INICIO = 2020
ANO_FIM = 2025
MIN_ESTACOES_AMOSTRA_PLENA = 3

COL_DATA = "Data"
COL_RADIACAO = "RADIACAO GLOBAL (Kj/m²)"
COL_TEMPERATURA = "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)"
COL_PRECIPITACAO = "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)"

NOME_ARQUIVO_RE = re.compile(r"^INMET_[A-Z]{2}_([A-Z]{2})_")


def processar_estacao(zip_ref: zipfile.ZipFile, nome_arquivo: str) -> pd.DataFrame | None:
    match = NOME_ARQUIVO_RE.match(nome_arquivo)
    if not match:
        return None
    uf = match.group(1)

    with zip_ref.open(nome_arquivo) as f:
        raw_bytes = f.read()

    try:
        df = pd.read_csv(
            BytesIO(raw_bytes), sep=";", encoding="latin1", decimal=",",
            skiprows=8, usecols=[COL_DATA, COL_RADIACAO, COL_TEMPERATURA, COL_PRECIPITACAO],
            na_values=["", "-9999", "-9999,0"],
        )
    except (ValueError, KeyError):
        # arquivo sem alguma coluna esperada (estação sem sensor de radiação, p.ex.)
        return None

    if df.empty:
        return None

    df["ano_mes"] = pd.to_datetime(df[COL_DATA], errors="coerce").dt.to_period("M")
    df = df.dropna(subset=["ano_mes"])

    agg = (
        df.groupby("ano_mes")
        .agg(
            irradiacao_media_kjm2=(COL_RADIACAO, "mean"),
            temperatura_media_c=(COL_TEMPERATURA, "mean"),
            precipitacao_total_mm=(COL_PRECIPITACAO, "sum"),
        )
        .reset_index()
    )
    agg["cod_uf"] = uf
    return agg


def processar_ano(ano: int) -> pd.DataFrame:
    zip_path = RAW_DIR / f"{ano}.zip"
    if not zip_path.exists():
        print(f"[transform_clima_uf] aviso: {zip_path} não encontrado — pulando ano {ano}", file=sys.stderr)
        return pd.DataFrame()

    print(f"[transform_clima_uf] processando {ano}.zip ...")
    resultados = []
    with zipfile.ZipFile(zip_path) as zf:
        nomes = [n for n in zf.namelist() if n.upper().endswith(".CSV")]
        for i, nome in enumerate(nomes, 1):
            r = processar_estacao(zf, nome)
            if r is not None:
                resultados.append(r)
            if i % 100 == 0:
                print(f"[transform_clima_uf]   {ano}: {i}/{len(nomes)} estações processadas...")

    if not resultados:
        return pd.DataFrame()

    por_estacao = pd.concat(resultados, ignore_index=True)
    por_estacao["id_tempo"] = por_estacao["ano_mes"].apply(lambda p: p.year * 100 + p.month)

    uf_mes = (
        por_estacao.groupby(["cod_uf", "id_tempo"])
        .agg(
            irradiacao_media_kjm2=("irradiacao_media_kjm2", "mean"),
            temperatura_media_c=("temperatura_media_c", "mean"),
            precipitacao_total_mm=("precipitacao_total_mm", "mean"),
            qtd_estacoes=("cod_uf", "count"),
        )
        .reset_index()
    )
    print(f"[transform_clima_uf] {ano}: {len(uf_mes)} combinações UF-mês geradas de {len(nomes)} estações")
    return uf_mes


def transformar() -> None:
    partes = [processar_ano(ano) for ano in range(ANO_INICIO, ANO_FIM + 1)]
    partes = [p for p in partes if not p.empty]
    if not partes:
        print("[transform_clima_uf] ERRO: nenhum dado processado — confira se os ZIPs do INMET foram baixados.", file=sys.stderr)
        sys.exit(1)

    dim_clima = pd.concat(partes, ignore_index=True)
    dim_clima["amostra_reduzida"] = dim_clima["qtd_estacoes"] < MIN_ESTACOES_AMOSTRA_PLENA
    for col in ["irradiacao_media_kjm2", "temperatura_media_c", "precipitacao_total_mm"]:
        dim_clima[col] = dim_clima[col].round(2)

    dim_clima.to_csv(TRUSTED_DIR / "dim_clima_uf.csv", index=False)
    print(f"[transform_clima_uf] dim_clima_uf: {len(dim_clima):,} linhas (UF x mês) salvas")
    print("[transform_clima_uf] concluído.")


if __name__ == "__main__":
    TRUSTED_DIR.mkdir(parents=True, exist_ok=True)
    transformar()
