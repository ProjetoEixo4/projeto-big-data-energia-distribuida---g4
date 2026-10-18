"""
Transformação — ANEEL Tarifas → fato_tarifa

Pergunta que a tabela responde: quanto custa, em média, o kWh para o consumidor
residencial de cada UF em cada mês, em valores nominais e em valores constantes?

Recorte tarifário (cada linha abaixo elimina uma duplicidade real do arquivo da
ANEEL, que traz ~328 mil linhas para ~2 mil tarifas "relevantes"):
    DscSubGrupo == B1                      baixa tensão residencial
    DscClasse == Residencial, DscSubClasse == Residencial   (exclui Baixa Renda e Tarifa Social)
    DscModalidadeTarifaria == Convencional (exclui Branca e pré-pagamento)
    DscBaseTarifaria == 'Tarifa de Aplicação'  (o que a conta de luz cobra; exclui 'Base Econômica')
    DscDetalhe == NomPostoTarifario == 'Não se aplica'   (exclui o componente SCEE e postos horários)

Cálculo:
    1. tarifa (R$/kWh) = (VlrTUSD + VlrTE) / 1000 (a ANEEL publica em R$/MWh).
    2. Cada reajuste vale a partir de DatInicioVigencia até o próximo reajuste da
       mesma distribuidora (as vigências do arquivo têm 6 sobreposições e 2 lacunas;
       "vale o último reajuste já iniciado" resolve ambas).
    3. Série DIÁRIA por distribuidora, média por mês (ponderada pelos dias de
       cada tarifa dentro do mês — um reajuste no dia 20 pesa 1/3 no mês).
    4. Distribuidora → UF: uma distribuidora pode atender mais de uma UF.
       A tarifa da UF é a média das distribuidoras que a atendem, PONDERADA pelo
       nº de usinas solares conectadas à distribuidora naquela UF (proxy da base
       de consumidores; pesos fixos no tempo para a série não oscilar por peso).
    5. Deflação: tarifa_deflacionada = tarifa × IPCA(dez/2025) / IPCA(mês),
       ou seja, em reais de dez/2025 (IPCA, SIDRA 1737). Sem IPCA disponível
       o script falha — a coluna não é mais uma cópia da nominal.

Depende de transform_geracao_uf.py (dim_distribuidora_uf) e de extract_ipca.py.

Uso:
    python transform/transform_tarifa.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from oet.io import ler_trusted, raw_dir, salvar_relatorio, salvar_trusted  # noqa: E402

ANO_INICIO, ANO_FIM = 2020, 2025
ARQ_TARIFAS = "tarifas-homologadas-distribuidoras-energia-eletrica.csv"
COLS = ["NumCNPJDistribuidora", "DatInicioVigencia", "DscSubGrupo", "DscClasse", "DscSubClasse",
        "DscModalidadeTarifaria", "DscBaseTarifaria", "DscDetalhe", "NomPostoTarifario",
        "VlrTUSD", "VlrTE"]
NAO_SE_APLICA = "Não se aplica"


def filtrar_b1_residencial(df: pd.DataFrame) -> pd.DataFrame:
    m = (
        (df["DscSubGrupo"] == "B1") & (df["DscClasse"] == "Residencial")
        & (df["DscSubClasse"] == "Residencial") & (df["DscModalidadeTarifaria"] == "Convencional")
        & (df["DscBaseTarifaria"] == "Tarifa de Aplicação")
        & (df["DscDetalhe"] == NAO_SE_APLICA) & (df["NomPostoTarifario"] == NAO_SE_APLICA)
    )
    return df[m].copy()


def carregar_ipca(caminho: Path) -> pd.Series:
    """Número-índice do IPCA indexado por id_tempo (AAAAMM)."""
    dados = json.loads(caminho.read_text(encoding="utf-8"))[1:]  # [0] é o cabeçalho da SIDRA
    s = pd.Series({int(d["D3C"]): float(d["V"]) for d in dados}).sort_index()
    if s.empty:
        raise RuntimeError(f"IPCA vazio em {caminho}")
    return s


def tarifa_mensal_por_distribuidora(df: pd.DataFrame) -> pd.DataFrame:
    """Média mensal ponderada por dias, por distribuidora (aplica 'vale o último reajuste')."""
    df = df.copy()
    df["inicio"] = pd.to_datetime(df["DatInicioVigencia"], errors="coerce")
    df["tarifa"] = (pd.to_numeric(df["VlrTUSD"], errors="coerce").fillna(0)
                    + pd.to_numeric(df["VlrTE"], errors="coerce").fillna(0)) / 1000.0
    df = df.dropna(subset=["inicio"]).sort_values(["NumCNPJDistribuidora", "inicio"])
    # duplicidade exata de início: fica a última linha publicada
    df = df.drop_duplicates(["NumCNPJDistribuidora", "inicio"], keep="last")

    dias = pd.DataFrame({"dia": pd.date_range(f"{ANO_INICIO}-01-01", f"{ANO_FIM}-12-31", freq="D")})
    saida = []
    for cnpj, g in df.groupby("NumCNPJDistribuidora"):
        d = pd.merge_asof(dias, g[["inicio", "tarifa"]], left_on="dia", right_on="inicio", direction="backward")
        d = d.dropna(subset=["tarifa"])  # dias anteriores à 1ª tarifa conhecida
        if d.empty:
            continue
        d["id_tempo"] = d["dia"].dt.year * 100 + d["dia"].dt.month
        m = d.groupby("id_tempo")["tarifa"].mean().reset_index()
        m["cod_distribuidora"] = str(cnpj).zfill(14)
        saida.append(m)
    return pd.concat(saida, ignore_index=True)


def agregar_por_uf(mensal: pd.DataFrame, pesos: pd.DataFrame) -> pd.DataFrame:
    x = mensal.merge(pesos[["cod_distribuidora", "cod_uf", "qtd_usinas"]], on="cod_distribuidora", how="inner")
    x["w_t"] = x["tarifa"] * x["qtd_usinas"]
    agg = x.groupby(["id_tempo", "cod_uf"]).agg(w_t=("w_t", "sum"), w=("qtd_usinas", "sum"),
                                                n_dist=("cod_distribuidora", "nunique")).reset_index()
    agg["tarifa_media_reais_kwh"] = agg["w_t"] / agg["w"]
    return agg[["id_tempo", "cod_uf", "tarifa_media_reais_kwh", "n_dist"]]


def deflacionar(fato: pd.DataFrame, ipca: pd.Series) -> pd.DataFrame:
    base = ipca.loc[ipca.index.max()]  # último mês disponível (dez/2025)
    faltam = sorted(set(fato["id_tempo"]) - set(ipca.index))
    if faltam:
        raise RuntimeError(f"IPCA sem os meses {faltam[:5]}... — rode etl/extract_ipca.py")
    fato = fato.copy()
    fato["tarifa_deflacionada"] = fato["tarifa_media_reais_kwh"] * base / fato["id_tempo"].map(ipca)
    return fato


def transformar() -> dict:
    csv = raw_dir("aneel_tarifas") / ARQ_TARIFAS
    ipca_arq = raw_dir("ibge_ipca") / "ipca_sidra_1737.json"
    for p, dica in [(csv, "etl/extract_aneel_tarifas.py"), (ipca_arq, "etl/extract_ipca.py")]:
        if not p.exists():
            raise FileNotFoundError(f"{p} não encontrado — rode {dica}")

    print(f"[transform_tarifa] lendo {csv} ...")
    df = pd.read_csv(csv, sep=";", encoding="utf-8", usecols=COLS, dtype=str)
    n_bruto = len(df)
    df = filtrar_b1_residencial(df).assign(
        VlrTUSD=lambda d: d["VlrTUSD"].str.replace(",", "."), VlrTE=lambda d: d["VlrTE"].str.replace(",", "."))
    print(f"[transform_tarifa] {n_bruto:,} linhas -> {len(df):,} tarifas B1 residenciais convencionais")

    mensal = tarifa_mensal_por_distribuidora(df)
    pesos = ler_trusted("dim_distribuidora_uf")
    fato = agregar_por_uf(mensal, pesos)
    fato = deflacionar(fato, carregar_ipca(ipca_arq))
    for c in ("tarifa_media_reais_kwh", "tarifa_deflacionada"):
        fato[c] = fato[c].round(4)
    fato = fato.rename(columns={"n_dist": "qtd_distribuidoras"}).sort_values(["cod_uf", "id_tempo"]).reset_index(drop=True)
    salvar_trusted(fato, "fato_tarifa")

    rel = {
        "linhas_brutas": n_bruto, "tarifas_filtradas": int(len(df)),
        "distribuidoras_com_tarifa": int(mensal["cod_distribuidora"].nunique()),
        "linhas_fato": int(len(fato)), "ufs": int(fato["cod_uf"].nunique()),
        "meses": int(fato["id_tempo"].nunique()),
        "ufs_sem_tarifa": sorted(set(ler_trusted("dim_uf")["cod_uf"]) - set(fato["cod_uf"])),
        "tarifa_nominal_min_max": [float(fato["tarifa_media_reais_kwh"].min()), float(fato["tarifa_media_reais_kwh"].max())],
    }
    salvar_relatorio("tarifa", rel)
    print(f"[transform_tarifa] fato_tarifa: {len(fato):,} linhas — {rel}")
    return rel


if __name__ == "__main__":
    try:
        transformar()
    except (FileNotFoundError, RuntimeError) as e:
        print(f"[transform_tarifa] ERRO: {e}", file=sys.stderr)
        sys.exit(1)
