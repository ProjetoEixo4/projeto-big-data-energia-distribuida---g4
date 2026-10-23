"""
Transformação — ANEEL Geração Distribuída → fato_geracao_uf (+ dim_distribuidora,
dim_distribuidora_uf)

O arquivo bruto (empreendimento-geracao-distribuida.parquet) é um CADASTRO: uma
fotografia atual das usinas, sem data de conexão. Para reconstruir a série
mensal de 2020 a 2025, o pipeline busca essa data nesta ordem de preferência:

    1. DatConexao da tabela técnica fotovoltaica da ANEEL
       (extract_aneel_gd_tecnico.py), cruzada por CodEmpreendimento =
       CodGeracaoDistribuida — essa é a data real de conexão.
    2. Como fallback (só quando a usina não tem correspondência, ou o arquivo
       técnico não foi baixado), usamos DthAtualizaCadastralEmpreend. Essa
       coluna mede a última atualização do cadastro, não a conexão em si.
       O relatório de qualidade mostra quanto % das usinas e da potência caiu
       em cada caso; sem o arquivo técnico, a série inteira é marcada como
       `serie_aproximada = true`.

Decisões de modelagem (todas documentadas no relatório de qualidade):
    - Escopo SOLAR: só entra SigTipoGeracao == 'UFV' (99,98% do cadastro). O
      projeto é sobre energia solar distribuída, então misturar eólica,
      térmica e hidro (os ~0,02% restantes) só distorceria o conceito, sem
      agregar nada à análise.
    - O fato representa um ESTOQUE ACUMULADO mensal, não

Uso:
    python transform/transform_geracao_uf.py
Gera (Parquet, camada trusted):
    dim_distribuidora, dim_distribuidora_uf, fato_geracao_uf, _qualidade_geracao.json
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from oet.io import ler_trusted, raw_dir, salvar_relatorio, salvar_trusted  # noqa: E402

ANO_INICIO, ANO_FIM = 2020, 2025
DATA_MIN_VALIDA = pd.Timestamp("2012-04-17")   # REN 482/2012 em vigor
PRIMEIRO_MES = pd.Period(f"{ANO_INICIO}-01", "M")
ULTIMO_MES = pd.Period(f"{ANO_FIM}-12", "M")

COLUNAS = [
    "NumCNPJDistribuidora", "NomAgente", "SigUF", "SigTipoGeracao",
    "DthAtualizaCadastralEmpreend", "DscModalidadeHabilitado", "DscClasseConsumo",
    "MdaPotenciaInstaladaKW", "CodEmpreendimento",
]

_MODALIDADE_NORM = {
    "geracao na propria uc": "Geração na própria UC",
    "auto consumo remoto": "Autoconsumo remoto",
    "autoconsumo remoto": "Autoconsumo remoto",
    "compartilhada": "Compartilhada",
    "condominio": "Condomínio",
    "condomínio": "Condomínio",
}
_CLASSE_NORM = {
    "residencial": "Residencial", "rebr": "Residencial baixa renda", "comercial": "Comercial",
    "industrial": "Industrial", "rural": "Rural", "poder público": "Poder Público",
    "serviço público": "Serviço Público", "iluminação pública": "Iluminação pública",
    "consumo próprio": "Consumo próprio",
}


def normalizar_modalidade(s: pd.Series) -> pd.Series:
    return s.fillna("").str.strip().str.lower().map(_MODALIDADE_NORM).fillna("Não informado")


def normalizar_classe(s: pd.Series) -> pd.Series:
    return s.fillna("").str.strip().str.lower().map(_CLASSE_NORM).fillna("Outras")


def carregar_datas_conexao() -> pd.DataFrame | None:
    arquivos = sorted(raw_dir("aneel_gd_tecnico").glob("*.parquet"))
    if not arquivos:
        return None
    df = pd.read_parquet(arquivos[-1])
    cols = {c.lower(): c for c in df.columns}
    c_cod, c_dat = cols.get("codgeracaodistribuida"), cols.get("datconexao")
    if c_cod is None or c_dat is None:
        raise RuntimeError(f"arquivo técnico sem CodGeracaoDistribuida/DatConexao: {list(df.columns)}")
    out = df[[c_cod, c_dat]].rename(columns={c_cod: "CodEmpreendimento", c_dat: "DatConexao"})
    out["DatConexao"] = pd.to_datetime(out["DatConexao"], errors="coerce")
    return out.dropna(subset=["CodEmpreendimento"]).drop_duplicates("CodEmpreendimento")


def definir_mes_de_entrada(df: pd.DataFrame, tecnico: pd.DataFrame | None) -> tuple[pd.DataFrame, dict]:
    """Atribui `mes_entrada` (Period M) a cada usina e informa a origem da data."""
    df = df.copy()
    df["dt_cadastro"] = pd.to_datetime(df["DthAtualizaCadastralEmpreend"], errors="coerce")
    df["dt_conexao"] = pd.NaT
    if tecnico is not None:
        df = df.merge(tecnico, on="CodEmpreendimento", how="left")
        df["dt_conexao"] = df["DatConexao"]
        df = df.drop(columns="DatConexao")

    limite = ULTIMO_MES.end_time
    ok_con = df["dt_conexao"].between(DATA_MIN_VALIDA, limite)
    ok_cad = df["dt_cadastro"].between(DATA_MIN_VALIDA, limite)
    df["origem_data"] = np.where(ok_con, "conexao", np.where(ok_cad, "cadastro", "invalida"))
    df["dt_ref"] = df["dt_conexao"].where(ok_con, df["dt_cadastro"].where(ok_cad))
    df = df.dropna(subset=["dt_ref"])  # sem nenhuma data utilizável
    df["mes_entrada"] = df["dt_ref"].dt.to_period("M")

    pot = df["MdaPotenciaInstaladaKW"].fillna(0)
    resumo = {
        "usinas_com_data": int(len(df)),
        "pct_usinas_data_conexao": float(round(100 * (df["origem_data"] == "conexao").mean(), 2)) if len(df) else 0.0,
        "pct_potencia_data_conexao": float(round(100 * pot[df["origem_data"] == "conexao"].sum() / pot.sum(), 2)) if pot.sum() else 0.0,
        "arquivo_tecnico_presente": tecnico is not None,
    }
    resumo["serie_aproximada"] = bool(resumo["pct_potencia_data_conexao"] < 90.0)
    return df, resumo


def construir_fato(df: pd.DataFrame) -> pd.DataFrame:
    """Estoque acumulado + fluxo mensal por (id_tempo, cod_uf, id_modalidade)."""
    dim_mod = ler_trusted("dim_modalidade")
    chave = dim_mod.set_index(["modalidade_aneel", "classe_consumo"])["id_modalidade"]
    df = df.copy()
    df["modalidade_aneel"] = normalizar_modalidade(df["DscModalidadeHabilitado"])
    df["classe_consumo"] = normalizar_classe(df["DscClasseConsumo"])
    df["id_modalidade"] = chave.reindex(pd.MultiIndex.from_frame(df[["modalidade_aneel", "classe_consumo"]])).to_numpy()

    meses = pd.period_range(PRIMEIRO_MES, ULTIMO_MES, freq="M")
    ids = [m.year * 100 + m.month for m in meses]
    df["pot"] = df["MdaPotenciaInstaladaKW"].fillna(0.0)
    df["mes_idx"] = (df["mes_entrada"] - PRIMEIRO_MES).apply(lambda x: x.n)

    # saldo anterior a jan/2020 (mes_idx < 0) e fluxo por mês (0..71)
    antes = df[df["mes_idx"] < 0].groupby(["SigUF", "id_modalidade"]).agg(pot0=("pot", "sum"), qtd0=("pot", "size"))
    dentro = df[df["mes_idx"] >= 0]
    fluxo = dentro.groupby(["SigUF", "id_modalidade", "mes_idx"]).agg(pot_nova=("pot", "sum"), qtd_nova=("pot", "size"))

    combos = df[["SigUF", "id_modalidade"]].drop_duplicates()
    grade = (combos.assign(k=1).merge(pd.DataFrame({"mes_idx": range(len(meses)), "k": 1}), on="k").drop(columns="k"))
    grade = grade.merge(fluxo.reset_index(), on=["SigUF", "id_modalidade", "mes_idx"], how="left")
    grade[["pot_nova", "qtd_nova"]] = grade[["pot_nova", "qtd_nova"]].fillna(0)
    grade = grade.merge(antes.reset_index(), on=["SigUF", "id_modalidade"], how="left")
    grade[["pot0", "qtd0"]] = grade[["pot0", "qtd0"]].fillna(0)

    grade = grade.sort_values(["SigUF", "id_modalidade", "mes_idx"])
    g = grade.groupby(["SigUF", "id_modalidade"])
    grade["potencia_acumulada_kw"] = grade["pot0"] + g["pot_nova"].cumsum()
    grade["qtd_acumulada"] = (grade["qtd0"] + g["qtd_nova"].cumsum()).astype("int64")
    grade["id_tempo"] = grade["mes_idx"].map(dict(enumerate(ids)))

    fato = grade.rename(columns={"SigUF": "cod_uf", "pot_nova": "potencia_nova_kw", "qtd_nova": "qtd_novas_conexoes"})
    fato["qtd_novas_conexoes"] = fato["qtd_novas_conexoes"].astype("int64")
    fato["potencia_media_kw"] = np.where(
        fato["qtd_acumulada"] > 0, fato["potencia_acumulada_kw"] / fato["qtd_acumulada"].replace(0, np.nan), 0.0)
    for c in ("potencia_acumulada_kw", "potencia_nova_kw", "potencia_media_kw"):
        fato[c] = fato[c].round(2)
    # combinações ainda vazias no mês (estoque 0) não agregam informação
    fato = fato[fato["qtd_acumulada"] > 0]
    return fato[["id_tempo", "cod_uf", "id_modalidade", "potencia_acumulada_kw", "qtd_acumulada",
                 "potencia_nova_kw", "qtd_novas_conexoes", "potencia_media_kw"]].reset_index(drop=True)


def construir_dims_distribuidora(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    d = df.dropna(subset=["NumCNPJDistribuidora", "SigUF"]).copy()
    d["cod_distribuidora"] = d["NumCNPJDistribuidora"].astype("int64").astype(str).str.zfill(14)
    por_uf = d.groupby(["cod_distribuidora", "SigUF"]).size().rename("qtd_usinas").reset_index()
    por_uf = por_uf.rename(columns={"SigUF": "cod_uf"})
    nome = d.groupby("cod_distribuidora")["NomAgente"].agg(lambda s: s.mode().iat[0] if not s.mode().empty else s.iloc[0])
    principal = por_uf.sort_values(["qtd_usinas", "cod_uf"], ascending=[False, True]).drop_duplicates("cod_distribuidora").set_index("cod_distribuidora")["cod_uf"]
    dim = pd.DataFrame({"nome_distribuidora": nome, "cod_uf_principal": principal}).reset_index()
    return dim, por_uf


def transformar(arquivo: Path | None = None) -> dict:
    arquivo = arquivo or next(iter(sorted(raw_dir("aneel_gd").glob("*.parquet"))), None)
    if arquivo is None:
        raise FileNotFoundError("parquet de GD não encontrado em raw/aneel_gd — rode etl/extract_aneel_gd.py")
    print(f"[transform_geracao_uf] lendo {arquivo} ...")
    bruto = pd.read_parquet(arquivo, columns=COLUNAS)
    total = len(bruto)
    solar = bruto[bruto["SigTipoGeracao"] == "UFV"].dropna(subset=["SigUF", "CodEmpreendimento"])
    print(f"[transform_geracao_uf] {total:,} registros; {len(solar):,} usinas fotovoltaicas")

    dim_dist, dist_uf = construir_dims_distribuidora(solar)
    salvar_trusted(dim_dist, "dim_distribuidora")
    salvar_trusted(dist_uf, "dim_distribuidora_uf")

    tecnico = carregar_datas_conexao()
    if tecnico is None:
        print("[transform_geracao_uf] AVISO: tabela técnica (DatConexao) ausente — usando data de "
              "atualização cadastral como aproximação. Rode etl/extract_aneel_gd_tecnico.py.", file=sys.stderr)
    com_data, resumo = definir_mes_de_entrada(solar, tecnico)
    fato = construir_fato(com_data)
    salvar_trusted(fato, "fato_geracao_uf")

    ultimo = fato[fato["id_tempo"] == fato["id_tempo"].max()]
    rel = {
        "registros_brutos": total, "usinas_ufv": int(len(solar)),
        "pct_ufv": round(100 * len(solar) / total, 3),
        "linhas_fato": int(len(fato)), "ufs": int(fato["cod_uf"].nunique()),
        "ultimo_id_tempo": int(fato["id_tempo"].max()),
        "potencia_acumulada_final_mw": round(float(ultimo["potencia_acumulada_kw"].sum()) / 1000, 1),
        "qtd_acumulada_final": int(ultimo["qtd_acumulada"].sum()),
        **resumo,
    }
    salvar_relatorio("geracao", rel)
    print(f"[transform_geracao_uf] fato_geracao_uf: {len(fato):,} linhas — relatório: {rel}")
    return rel


if __name__ == "__main__":
    try:
        transformar()
    except (FileNotFoundError, RuntimeError) as e:
        print(f"[transform_geracao_uf] ERRO: {e}", file=sys.stderr)
        sys.exit(1)
