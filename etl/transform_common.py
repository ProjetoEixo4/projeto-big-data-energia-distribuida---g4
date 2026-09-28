"""
Transformação — camada raw -> trusted
Lê os arquivos extraídos em data/raw/, padroniza chaves e tipos, trata nulos
e grava em data/trusted/<entidade>/ como Parquet particionado por ano (e UF,
quando aplicável). Em produção (AWS), a mesma lógica roda como um AWS Glue Job
(ver glue/glue_job_aneel_gd.py) lendo/gravando em S3 em vez de disco local.


import os

import pandas as pd

RAW_DIR = os.path.join("data", "raw")
TRUSTED_DIR = os.path.join("data", "trusted")
ANO_INICIO, ANO_FIM = 2020, 2025

# Mapa de modalidade ANEEL -> tipo de consumidor (regra de negócio central do projeto)
MODALIDADE_PARA_TIPO = {
    "Geração na própria UC": "Unifamiliar",
    "Autoconsumo remoto": "Unifamiliar",
    "Geração compartilhada": "Multifamiliar",
    "Empreendimento com múltiplas unidades consumidoras": "Multifamiliar",
}


def classificar_tipo_consumidor(modalidade: str, classe_consumo: str) -> str:
    """Deriva Unifamiliar / Multifamiliar / Rural / Outro a partir dos campos
    brutos da ANEEL (classe de consumo tem prioridade para o caso Rural)."""
    if isinstance(classe_consumo, str) and classe_consumo.strip().lower() == "rural":
        return "Rural"
    return MODALIDADE_PARA_TIPO.get(str(modalidade).strip(), "Outro")


def transformar_aneel_gd(caminho_csv: str) -> pd.DataFrame:
    df = pd.read_csv(caminho_csv, sep=";", encoding="latin1", low_memory=False)
    df.columns = [c.strip().upper() for c in df.columns]

    # --- Tratamento de nulos (critério por campo) ---
    # Potência é a medida central do fato: registro sem potência é descartado.
    antes = len(df)
    df = df.dropna(subset=["MDA_POTENCIAINSTALADAKW"])
    print(f"[transform] {antes - len(df)} registro(s) descartado(s) por falta de potência instalada")

    # Município ausente/inválido -> aponta para o membro "Não identificado" da dimensão
    df["COD_MUNICIPIO_IBGE"] = df.get("COD_MUNICIPIO_IBGE", pd.NA).fillna("0000000")

    # Distribuidora ausente -> aponta para o membro "Não identificada"
    df["COD_DISTRIBUIDORA"] = df.get("SIG_AGENTE", pd.NA).fillna("DESCONHECIDA")

    # --- Derivações ---
    df["TIPO_CONSUMIDOR"] = df.apply(
        lambda r: classificar_tipo_consumidor(
            r.get("DSC_MODALIDADE", ""), r.get("DSC_CLASSE_CONSUMO", "")
        ),
        axis=1,
    )
    df["ANO_CONEXAO"] = pd.to_datetime(
        df.get("DTH_ATUALIZACAOCADASTRAL"), errors="coerce"
    ).dt.year

    # --- Filtro temporal do projeto: mantém apenas 2020-2025 ---
    antes_filtro = len(df)
    df = df[(df["ANO_CONEXAO"] >= ANO_INICIO) & (df["ANO_CONEXAO"] <= ANO_FIM)]
    print(f"[transform] {antes_filtro - len(df)} registro(s) fora do recorte {ANO_INICIO}-{ANO_FIM} descartado(s)")

    return df


def gravar_particionado(df: pd.DataFrame, entidade: str, col_particao: str = "ANO_CONEXAO") -> None:
    destino = os.path.join(TRUSTED_DIR, entidade)
    os.makedirs(destino, exist_ok=True)
    df.to_parquet(destino, partition_cols=[col_particao], index=False)
    print(f"[transform] gravado em {destino}, particionado por {col_particao}")


if __name__ == "__main__":
    import glob

    arquivos = sorted(glob.glob(os.path.join(RAW_DIR, "aneel_gd", "*.csv")))
    if not arquivos:
        raise SystemExit("Nenhum arquivo bruto da ANEEL GD encontrado — rode extract_aneel_gd.py antes.")

    df_gd = transformar_aneel_gd(arquivos[-1])
    gravar_particionado(df_gd, entidade="fato_geracao_distribuida")
