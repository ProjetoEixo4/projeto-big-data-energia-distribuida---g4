"""
Transformação — ANEEL Tarifas → fato_tarifa

O CSV de tarifas homologadas não tem coluna de UF — só a sigla e o CNPJ da
distribuidora. Por isso este script DEPENDE do dim_distribuidora.csv gerado
por transform_geracao_uf.py (que constrói o mapa CNPJ -> UF a partir da base
de Geração Distribuída, cruzando pelo mesmo CNPJ usado aqui). Rode
transform_geracao_uf.py ANTES deste script.

Simplificações assumidas:
    - Considera-se apenas o subgrupo B1 (baixa tensão, consumidor residencial
      "padrão"), que é o subgrupo mais numeroso e o mais representativo para
      uma comparação de tarifa média por UF voltada ao consumidor residencial
      — público-alvo típico da geração distribuída/microgeração solar.
    - VlrTUSD e VlrTE vêm em R$/MWh (confirmado pela ordem de grandeza dos
      dados brutos); a tarifa em R$/kWh é (TUSD + TE) / 1000.
    - tarifa_deflacionada: como o projeto não incorpora aqui uma série de
      índice de preços (IPCA) por ano, usamos a própria tarifa nominal como
      aproximação de primeira ordem (tarifa_deflacionada = tarifa nominal).
      Fica registrado como ponto de melhoria futura no README.

Uso:
    python transform/transform_tarifa.py
Gera:
    data/trusted/fato_tarifa.csv
"""
import sys
from pathlib import Path

import pandas as pd

RAW_FILE = Path("data/raw/aneel_tarifas/tarifas-homologadas-distribuidoras-energia-eletrica.csv")
DIST_FILE = Path("data/trusted/dim_distribuidora.csv")
TRUSTED_DIR = Path("data/trusted")

ANO_INICIO = 2020
ANO_FIM = 2025
SUBGRUPO_ALVO = "B1"


def transformar() -> None:
    if not RAW_FILE.exists():
        print(f"[transform_tarifa] ERRO: arquivo não encontrado: {RAW_FILE}. "
              "Rode etl/extract_aneel_tarifas.py primeiro.", file=sys.stderr)
        sys.exit(1)
    if not DIST_FILE.exists():
        print(f"[transform_tarifa] ERRO: {DIST_FILE} não existe. "
              "Rode transform/transform_geracao_uf.py primeiro (ele gera o mapa CNPJ -> UF).", file=sys.stderr)
        sys.exit(1)

    print(f"[transform_tarifa] lendo {RAW_FILE} ...")
    df = pd.read_csv(
        RAW_FILE, sep=";", encoding="utf-8", decimal=",", quotechar='"',
        usecols=["NumCNPJDistribuidora", "DatInicioVigencia", "DscSubGrupo", "VlrTUSD", "VlrTE"],
        dtype={"NumCNPJDistribuidora": str, "DatInicioVigencia": str, "DscSubGrupo": str},
    )
    print(f"[transform_tarifa] {len(df):,} registros lidos")

    dist = pd.read_csv(DIST_FILE, dtype={"cod_distribuidora": str})
    cnpj_para_uf = dict(zip(dist["cod_distribuidora"], dist["cod_uf_principal"]))

    df = df[df["DscSubGrupo"] == SUBGRUPO_ALVO].copy()
    df["cod_uf"] = df["NumCNPJDistribuidora"].map(cnpj_para_uf)
    df = df.dropna(subset=["cod_uf"])

    df["dt_ref"] = pd.to_datetime(df["DatInicioVigencia"], errors="coerce")
    df = df[df["dt_ref"].dt.year.between(ANO_INICIO, ANO_FIM)]
    df["id_tempo"] = df["dt_ref"].dt.year * 100 + df["dt_ref"].dt.month
    print(f"[transform_tarifa] {len(df):,} registros após filtro (subgrupo {SUBGRUPO_ALVO}, {ANO_INICIO}-{ANO_FIM})")

    df["tarifa_reais_kwh"] = (df["VlrTUSD"].fillna(0) + df["VlrTE"].fillna(0)) / 1000

    fato = (
        df.groupby(["id_tempo", "cod_uf"])
        .agg(tarifa_media_reais_kwh=("tarifa_reais_kwh", "mean"))
        .reset_index()
    )
    fato["tarifa_media_reais_kwh"] = fato["tarifa_media_reais_kwh"].round(4)
    # aproximação de primeira ordem — ver docstring
    fato["tarifa_deflacionada"] = fato["tarifa_media_reais_kwh"]

    fato.to_csv(TRUSTED_DIR / "fato_tarifa.csv", index=False)
    print(f"[transform_tarifa] fato_tarifa: {len(fato):,} linhas agregadas")
    print("[transform_tarifa] concluído.")


if __name__ == "__main__":
    TRUSTED_DIR.mkdir(parents=True, exist_ok=True)
    transformar()
