"""
Transformação — ANEEL Geração Distribuída → fato_geracao_uf (+ dim_distribuidora, dim_modalidade)

O arquivo bruto da ANEEL (empreendimento-geracao-distribuida.parquet) é um
CADASTRO — uma fotografia do estado atual de todas as usinas de geração
distribuída registradas, não uma série histórica mês a mês. Ele não tem uma
coluna de "data de conexão"; o campo mais próximo disso é
DthAtualizaCadastralEmpreend (data da última atualização cadastral do
empreendimento), que na prática reflete o mês em que o cadastro foi criado
ou modificado. Usamos esse campo como proxy do período de referência —
é a mesma abordagem usada em estudos setoriais que reconstroem a evolução
temporal da GD a partir dessa base cadastral, já que a ANEEL não publica
outra fonte com granularidade mensal completa 2020-2025.

Mapeamento de tipo_consumidor (dim_modalidade), simplificado a partir das
colunas disponíveis:
    - DscClasseConsumo == 'Rural'            -> 'Rural'
    - DscModalidadeHabilitado == 'Condomínio' -> 'Multifamiliar'
    - SigTipoConsumidor == 'PF' (pessoa física) -> 'Unifamiliar'
    - qualquer outro caso                     -> 'Outro'

Uso:
    python transform/transform_geracao_uf.py
Gera:
    data/trusted/dim_distribuidora.csv
    data/trusted/dim_modalidade.csv
    data/trusted/fato_geracao_uf.csv
"""
import sys
from pathlib import Path

import pandas as pd

RAW_FILE = Path("data/raw/aneel_gd/empreendimento-geracao-distribuida.parquet")
TRUSTED_DIR = Path("data/trusted")

ANO_INICIO = 2020
ANO_FIM = 2025

COLUNAS = [
    "NumCNPJDistribuidora", "SigAgente", "NomAgente", "SigUF",
    "DthAtualizaCadastralEmpreend", "DscModalidadeHabilitado",
    "DscClasseConsumo", "SigTipoConsumidor", "MdaPotenciaInstaladaKW",
    "CodEmpreendimento",
]


def classificar_tipo_consumidor(row) -> str:
    if row["DscClasseConsumo"] == "Rural":
        return "Rural"
    if row["DscModalidadeHabilitado"] == "Condomínio":
        return "Multifamiliar"
    if row["SigTipoConsumidor"] == "PF":
        return "Unifamiliar"
    return "Outro"


def transformar() -> None:
    if not RAW_FILE.exists():
        print(f"[transform_geracao_uf] ERRO: arquivo não encontrado: {RAW_FILE}. "
              "Rode etl/extract_aneel_gd.py primeiro.", file=sys.stderr)
        sys.exit(1)

    print(f"[transform_geracao_uf] lendo {RAW_FILE} ...")
    df = pd.read_parquet(RAW_FILE, engine="pyarrow", columns=COLUNAS)
    print(f"[transform_geracao_uf] {len(df):,} registros lidos")

    # --- dim_distribuidora: uma linha por CNPJ, UF = moda das UFs atendidas ---
    dist = (
        df.dropna(subset=["NumCNPJDistribuidora"])
        .groupby("NumCNPJDistribuidora")
        .agg(
            nome_distribuidora=("NomAgente", lambda s: s.mode().iat[0] if not s.mode().empty else s.iloc[0]),
            cod_uf_principal=("SigUF", lambda s: s.mode().iat[0] if not s.mode().empty else None),
        )
        .reset_index()
        .rename(columns={"NumCNPJDistribuidora": "cod_distribuidora"})
    )
    dist["cod_distribuidora"] = dist["cod_distribuidora"].astype(str)
    dist.to_csv(TRUSTED_DIR / "dim_distribuidora.csv", index=False)
    print(f"[transform_geracao_uf] dim_distribuidora: {len(dist)} distribuidoras")

    # --- filtro temporal (2020-2025), a partir do proxy DthAtualizaCadastralEmpreend ---
    df["dt_ref"] = pd.to_datetime(df["DthAtualizaCadastralEmpreend"], errors="coerce")
    df = df[df["dt_ref"].dt.year.between(ANO_INICIO, ANO_FIM)]
    df["id_tempo"] = df["dt_ref"].dt.year * 100 + df["dt_ref"].dt.month
    print(f"[transform_geracao_uf] {len(df):,} registros após filtro temporal {ANO_INICIO}-{ANO_FIM}")

    # --- dim_modalidade: combinações distintas observadas ---
    df["tipo_consumidor"] = df.apply(classificar_tipo_consumidor, axis=1)
    df["DscModalidadeHabilitado"] = df["DscModalidadeHabilitado"].fillna("Não informado")
    df["DscClasseConsumo"] = df["DscClasseConsumo"].fillna("Não informado")

    modalidades = (
        df[["DscModalidadeHabilitado", "DscClasseConsumo", "tipo_consumidor"]]
        .drop_duplicates()
        .rename(columns={
            "DscModalidadeHabilitado": "modalidade_aneel",
            "DscClasseConsumo": "classe_consumo",
        })
        .reset_index(drop=True)
    )
    modalidades.insert(0, "id_modalidade_natural", range(1, len(modalidades) + 1))
    modalidades.to_csv(TRUSTED_DIR / "dim_modalidade.csv", index=False)
    print(f"[transform_geracao_uf] dim_modalidade: {len(modalidades)} combinações distintas")

    df = df.merge(
        modalidades,
        left_on=["DscModalidadeHabilitado", "DscClasseConsumo", "tipo_consumidor"],
        right_on=["modalidade_aneel", "classe_consumo", "tipo_consumidor"],
        how="left",
    )

    # --- fato_geracao_uf: agregação por id_tempo x cod_uf x modalidade ---
    fato = (
        df.dropna(subset=["SigUF"])
        .groupby(["id_tempo", "SigUF", "id_modalidade_natural"])
        .agg(
            potencia_total_kw=("MdaPotenciaInstaladaKW", "sum"),
            qtd_conexoes=("CodEmpreendimento", "count"),
        )
        .reset_index()
        .rename(columns={"SigUF": "cod_uf"})
    )
    fato["potencia_media_kw"] = (fato["potencia_total_kw"] / fato["qtd_conexoes"]).round(2)
    fato["potencia_total_kw"] = fato["potencia_total_kw"].round(2)

    fato.to_csv(TRUSTED_DIR / "fato_geracao_uf.csv", index=False)
    print(f"[transform_geracao_uf] fato_geracao_uf: {len(fato):,} linhas agregadas")
    print("[transform_geracao_uf] concluído.")


if __name__ == "__main__":
    TRUSTED_DIR.mkdir(parents=True, exist_ok=True)
    transformar()
