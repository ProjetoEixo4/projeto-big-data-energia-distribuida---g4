"""
Transformação — INMET (clima) → dim_clima_uf

Cada ZIP anual do INMET traz ~600 CSVs, um por estação automática. Como o
modelo guarda clima por UF-mês, agregamos em três passos, sempre com
unidades compatíveis com geração solar:

    1. Por estação e dia: irradiação global diária em kWh/m²/dia (soma das
       leituras horárias em kJ/m² ÷ 3600). O INMET não grava a radiação à
       noite — deixa em branco, não zero. Por isso a validade do dia é
       checada só na janela diurna (10h–22h UTC, que cobre do Acre ao
       litoral): exige-se MIN_HORAS_DIA_VALIDO leituras nesse intervalo;
       fora dele, vazio vira 0. Totais acima de MAX_KWH_DIA são
       fisicamente impossíveis e tratados como erro de sensor.
    2. Por estação e mês: média dos dias válidos (irradiação), média da
       temperatura e soma da precipitação.
    3. Por UF e mês: média entre as estações da UF. Guardamos qtd_estacoes
       e cobertura_pct (percentual das horas diurnas do mês com radiação
       válida, média das estações) para quem for usar o dado saber o
       quanto confiar em cada UF-mês. amostra_reduzida marca menos de 3
       estações ou cobertura abaixo de 50%.

A UF é extraída do NOME do arquivo: INMET_<REGIÃO>_<UF>_<código>_... . A
região pode ter 1 ou 2 letras (CO, N, NE, S, SE) e, a partir de 2025, o
ZIP passou a guardar os arquivos dentro de uma pasta "2025/" — a regex
cobre os dois formatos.
"""

Uso:
    python transform/transform_clima_uf.py
Gera (Parquet, camada trusted): dim_clima_uf
"""
from __future__ import annotations

import re
import sys
import zipfile
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from oet.io import raw_dir, salvar_relatorio, salvar_trusted  # noqa: E402

RAW_DIR = raw_dir("inmet")

ANO_INICIO = 2020
ANO_FIM = 2025
MIN_ESTACOES_AMOSTRA_PLENA = 3
MIN_COBERTURA_AMOSTRA_PLENA = 50.0
JANELA_DIURNA_UTC = (10, 22)   # horas UTC inclusivas
MIN_HORAS_DIA_VALIDO = 10
MAX_KWH_DIA = 10.0

COL_DATA = "Data"
COL_HORA = "Hora UTC"
COL_RADIACAO = "RADIACAO GLOBAL (Kj/m²)"
COL_TEMPERATURA = "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)"
COL_PRECIPITACAO = "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)"

UFS_VALIDAS = {
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT",
    "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
}
NOME_ARQUIVO_RE = re.compile(r"^(?:.*/)?INMET_([A-Z]{1,2})_([A-Z]{2})_", re.IGNORECASE)


def uf_do_arquivo(nome_arquivo: str) -> str | None:
    m = NOME_ARQUIVO_RE.match(nome_arquivo)
    if not m:
        return None
    uf = m.group(2).upper()
    return uf if uf in UFS_VALIDAS else None


def processar_estacao_bytes(raw_bytes: bytes, uf: str) -> pd.DataFrame | None:
    """Agrega um CSV de estação para (ano_mes) e devolve None se inaproveitável."""
    try:
        df = pd.read_csv(
            BytesIO(raw_bytes), sep=";", encoding="latin1", decimal=",",
            skiprows=8, usecols=[COL_DATA, COL_HORA, COL_RADIACAO, COL_TEMPERATURA, COL_PRECIPITACAO],
            na_values=["", "-9999", "-9999,0"],
        )
    except (ValueError, KeyError):
        return None  # estação sem alguma das colunas esperadas
    if df.empty:
        return None

    df["data"] = pd.to_datetime(df[COL_DATA], format="mixed", errors="coerce").dt.normalize()
    df = df.dropna(subset=["data"])
    # leituras negativas de radiação são ruído do sensor
    df.loc[df[COL_RADIACAO] < 0, COL_RADIACAO] = np.nan
    hora = pd.to_numeric(df[COL_HORA].astype(str).str.extract(r"(\d{2})")[0], errors="coerce")
    na_janela = hora.between(*JANELA_DIURNA_UTC)
    df["rad_valida"] = df[COL_RADIACAO].notna() & na_janela

    diario = df.groupby("data").agg(
        rad_kj=(COL_RADIACAO, "sum"),
        horas_validas=("rad_valida", "sum"),
    )
    diario["irrad_kwh_m2"] = np.where(
        diario["horas_validas"] >= MIN_HORAS_DIA_VALIDO, diario["rad_kj"] / 3600.0, np.nan
    )
    diario.loc[diario["irrad_kwh_m2"] > MAX_KWH_DIA, "irrad_kwh_m2"] = np.nan
    diario["ano_mes"] = diario.index.to_period("M")
    irrad_mes = diario.groupby("ano_mes")["irrad_kwh_m2"].mean()

    df["ano_mes"] = df["data"].dt.to_period("M")
    mensal = df.groupby("ano_mes").agg(
        temperatura_media_c=(COL_TEMPERATURA, "mean"),
        precipitacao_total_mm=(COL_PRECIPITACAO, "sum"),
        horas_rad_validas=("rad_valida", "sum"),
        horas_lidas=("rad_valida", "size"),
    )
    mensal["irradiacao_kwh_m2_dia"] = irrad_mes
    n_horas_janela = JANELA_DIURNA_UTC[1] - JANELA_DIURNA_UTC[0] + 1
    horas_do_mes = mensal.index.to_series().apply(lambda p: p.days_in_month * n_horas_janela)
    mensal["cobertura_pct"] = 100.0 * mensal["horas_rad_validas"] / horas_do_mes
    mensal = mensal.reset_index()
    mensal["cod_uf"] = uf
    return mensal[["cod_uf", "ano_mes", "irradiacao_kwh_m2_dia", "temperatura_media_c",
                   "precipitacao_total_mm", "cobertura_pct"]]


def processar_ano(ano: int, raw_dir: Path = RAW_DIR) -> pd.DataFrame:
    zip_path = raw_dir / f"{ano}.zip"
    if not zip_path.exists():
        print(f"[transform_clima_uf] aviso: {zip_path} não encontrado — pulando {ano}", file=sys.stderr)
        return pd.DataFrame()

    print(f"[transform_clima_uf] processando {ano}.zip ...")
    resultados, descartados = [], 0
    with zipfile.ZipFile(zip_path) as zf:
        nomes = [n for n in zf.namelist() if n.upper().endswith(".CSV")]
        for i, nome in enumerate(nomes, 1):
            uf = uf_do_arquivo(nome)
            if uf is None:
                descartados += 1
                continue
            r = processar_estacao_bytes(zf.read(nome), uf)
            if r is None:
                descartados += 1
            else:
                resultados.append(r)
            if i % 150 == 0:
                print(f"[transform_clima_uf]   {ano}: {i}/{len(nomes)} estações")

    if not resultados:
        return pd.DataFrame()

    est = pd.concat(resultados, ignore_index=True)
    est["id_tempo"] = est["ano_mes"].apply(lambda p: p.year * 100 + p.month)
    uf_mes = (
        est.groupby(["cod_uf", "id_tempo"])
        .agg(
            irradiacao_kwh_m2_dia=("irradiacao_kwh_m2_dia", "mean"),
            temperatura_media_c=("temperatura_media_c", "mean"),
            precipitacao_total_mm=("precipitacao_total_mm", "mean"),
            cobertura_pct=("cobertura_pct", "mean"),
            qtd_estacoes=("cod_uf", "count"),
        )
        .reset_index()
    )
    print(f"[transform_clima_uf] {ano}: {len(uf_mes)} UF-mês de {len(nomes)} arquivos "
          f"({descartados} descartados)")
    return uf_mes


def construir(raw_dir: Path = RAW_DIR, anos=range(ANO_INICIO, ANO_FIM + 1)) -> pd.DataFrame:
    partes = [p for p in (processar_ano(a, raw_dir) for a in anos) if not p.empty]
    if not partes:
        raise RuntimeError("nenhum dado do INMET processado — confira se os ZIPs foram baixados")
    dim = pd.concat(partes, ignore_index=True)
    dim["amostra_reduzida"] = (
        (dim["qtd_estacoes"] < MIN_ESTACOES_AMOSTRA_PLENA)
        | (dim["cobertura_pct"] < MIN_COBERTURA_AMOSTRA_PLENA)
    )
    for col, casas in [("irradiacao_kwh_m2_dia", 3), ("temperatura_media_c", 2),
                       ("precipitacao_total_mm", 1), ("cobertura_pct", 1)]:
        dim[col] = dim[col].round(casas)
    return dim.sort_values(["cod_uf", "id_tempo"]).reset_index(drop=True)


def transformar() -> None:
    dim = construir()
    salvar_trusted(dim, "dim_clima_uf")
    salvar_relatorio("clima", {
        "linhas": int(len(dim)), "ufs": int(dim["cod_uf"].nunique()), "meses": int(dim["id_tempo"].nunique()),
        "amostra_reduzida": int(dim["amostra_reduzida"].sum()),
        "irradiacao_sem_valor": int(dim["irradiacao_kwh_m2_dia"].isna().sum()),
        "irradiacao_min_max": [float(dim["irradiacao_kwh_m2_dia"].min()), float(dim["irradiacao_kwh_m2_dia"].max())],
    })
    print(f"[transform_clima_uf] dim_clima_uf: {len(dim):,} linhas "
          f"({dim['cod_uf'].nunique()} UFs) salvas")


if __name__ == "__main__":
    try:
        transformar()
    except RuntimeError as e:
        print(f"[transform_clima_uf] ERRO: {e}", file=sys.stderr)
        sys.exit(1)
