"""
Transformação — EPE BEN (matriz energética nacional) → fato_matriz_energetica

Cada arquivo ben_AAAA.xlsx tem uma matriz nacional (não por UF) com uma linha
por etapa do balanço (Produção, Importação, Oferta Total, etc.) e uma coluna
por fonte de energia, agrupadas em "Fontes de Energia Primária_*" e "Fontes
de Energia Secundária_*". Os valores estão em mil tep (toneladas equivalentes
de petróleo).

Esta tabela fato é NACIONAL, não tem cod_uf — por isso não se junta a dim_uf
(consistente com o DDL: fato_matriz_energetica só referencia id_tempo e
cod_fonte). Usa-se a linha "Produção" (produção nacional de energia primária)
como base, reduzida às mesmas 8 fontes primárias definidas em
seed_dimensoes.py — CARVAO soma as colunas Carvão Vapor + Carvão Metalúrgico.

Conversão de unidade: 1 mil tep = 11,63 GWh (fator padrão de conversão
tep -> MWh: 1 tep = 11,63 MWh; logo 1.000 tep = 11.630 MWh = 11,63 GWh).

Uso:
    python transform/transform_matriz_energetica.py
Gera:
    data/trusted/fato_matriz_energetica.csv
"""
import glob
import re
import sys
from pathlib import Path

import pandas as pd

RAW_DIR = Path("data/raw/epe_ben")
TRUSTED_DIR = Path("data/trusted")

MIL_TEP_PARA_GWH = 11.63

# Mapeia cod_fonte (definido em seed_dimensoes.py) -> lista de colunas do BEN a somar
MAPA_FONTES = {
    "PETROLEO": ["Fontes de Energia Primária_Petróleo"],
    "GAS_NATURAL": ["Fontes de Energia Primária_Gás Natural"],
    "CARVAO": ["Fontes de Energia Primária_Carvão Vapor", "Fontes de Energia Primária_Carvão Metalúrgico"],
    "URANIO": ["Fontes de Energia Primária_Urânio U3o8"],
    "HIDRAULICA": ["Fontes de Energia Primária_Energia Hidráulica"],
    "LENHA": ["Fontes de Energia Primária_Lenha"],
    "CANA": ["Fontes de Energia Primária_Produtos da Cana"],
    "OUTRAS_PRIM": ["Fontes de Energia Primária_Outras Fontes Primárias"],
}

ANO_RE = re.compile(r"ben_(\d{4})\.xlsx$")


def extrair_ano(caminho: str) -> int | None:
    m = ANO_RE.search(caminho)
    return int(m.group(1)) if m else None


def processar_arquivo(caminho: Path, ano: int) -> list[dict]:
    df = pd.read_excel(caminho, sheet_name=0)
    linha_producao = df[df["grupo"] == "Produção"]
    if linha_producao.empty:
        print(f"[transform_matriz_energetica] aviso: linha 'Produção' não encontrada em {caminho}", file=sys.stderr)
        return []
    linha = linha_producao.iloc[0]

    valores = {}
    for cod_fonte, colunas in MAPA_FONTES.items():
        valores[cod_fonte] = sum(max(float(linha[c]), 0) for c in colunas if c in df.columns)

    total = sum(valores.values())
    if total == 0:
        return []

    id_tempo = ano * 100 + 12  # matriz é anual; convenciona-se dezembro como id_tempo de referência
    linhas = []
    for cod_fonte, valor_mil_tep in valores.items():
        linhas.append({
            "id_tempo": id_tempo,
            "cod_fonte": cod_fonte,
            "participacao_percentual": round(100 * valor_mil_tep / total, 2),
            "geracao_gwh": round(valor_mil_tep * MIL_TEP_PARA_GWH, 2),
        })
    return linhas


def transformar() -> None:
    arquivos = sorted(glob.glob(str(RAW_DIR / "ben_*.xlsx")))
    if not arquivos:
        print(f"[transform_matriz_energetica] ERRO: nenhum arquivo ben_*.xlsx encontrado em {RAW_DIR}. "
              "Rode etl/extract_epe_ben.py primeiro.", file=sys.stderr)
        sys.exit(1)

    todas_linhas = []
    for caminho in arquivos:
        ano = extrair_ano(caminho)
        if ano is None:
            continue
        print(f"[transform_matriz_energetica] processando {caminho} (ano {ano}) ...")
        todas_linhas.extend(processar_arquivo(Path(caminho), ano))

    if not todas_linhas:
        print("[transform_matriz_energetica] ERRO: nenhuma linha gerada.", file=sys.stderr)
        sys.exit(1)

    fato = pd.DataFrame(todas_linhas)
    fato.to_csv(TRUSTED_DIR / "fato_matriz_energetica.csv", index=False)
    print(f"[transform_matriz_energetica] fato_matriz_energetica: {len(fato)} linhas salvas")
    print("[transform_matriz_energetica] concluído.")


if __name__ == "__main__":
    TRUSTED_DIR.mkdir(parents=True, exist_ok=True)
    transformar()
