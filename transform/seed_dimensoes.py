"""
Transformação — Seed das dimensões estáticas: dim_tempo, dim_uf, dim_fonte_energia

dim_tempo: calendário mensal 2020-01 a 2025-12 (72 linhas). id_tempo no formato
AAAAMM (ex.: 202001), usado como chave em todas as tabelas fato/dim_clima.

dim_uf: as 27 UFs do Brasil. Nome, região, população e área vêm de uma tabela
de referência estática (Censo IBGE 2022 / malha territorial IBGE) — esses
valores mudam pouco e não justificam uma chamada de API própria. O PIB per
capita é CALCULADO a partir do PIB total extraído da API SIDRA (tabela 5938,
variável 37, nível UF) dividido pela população de referência, usando o ano
mais recente disponível no arquivo baixado por extract_ibge_contas_regionais.py.

dim_fonte_energia: as fontes primárias de energia do BEN (Balanço Energético
Nacional), reduzidas às 8 categorias principais usadas em fato_matriz_energetica
(ver transform_matriz_energetica.py para o detalhamento de por que essas 8).

Uso:
    python transform/seed_dimensoes.py
Gera:
    data/trusted/dim_tempo.csv
    data/trusted/dim_uf.csv
    data/trusted/dim_fonte_energia.csv
"""
import glob
import json
import sys
from pathlib import Path

import pandas as pd

RAW_IBGE_DIR = Path("data/raw/ibge_contas_regionais")
TRUSTED_DIR = Path("data/trusted")

ANO_INICIO = 2020
ANO_FIM = 2025

# Referência estática — Censo IBGE 2022 (população) e malha territorial IBGE
# (área em km²). Código D1C é o código numérico IBGE de UF usado pela API SIDRA;
# cod_uf é a sigla de 2 letras usada em todo o resto do projeto (ANEEL, INMET).
UF_REFERENCIA = [
    # (cod_ibge, cod_uf, nome_uf, regiao, populacao_2022, area_km2)
    ("11", "RO", "Rondônia", "Norte", 1581196, 237754.451),
    ("12", "AC", "Acre", "Norte", 830018, 164123.737),
    ("13", "AM", "Amazonas", "Norte", 3941613, 1559167.878),
    ("14", "RR", "Roraima", "Norte", 636707, 224273.831),
    ("15", "PA", "Pará", "Norte", 8120131, 1245870.707),
    ("16", "AP", "Amapá", "Norte", 733759, 142470.762),
    ("17", "TO", "Tocantins", "Norte", 1511460, 277720.520),
    ("21", "MA", "Maranhão", "Nordeste", 6776699, 329642.170),
    ("22", "PI", "Piauí", "Nordeste", 3271199, 251577.738),
    ("23", "CE", "Ceará", "Nordeste", 8794957, 148894.447),
    ("24", "RN", "Rio Grande do Norte", "Nordeste", 3302729, 52809.599),
    ("25", "PB", "Paraíba", "Nordeste", 3974687, 56467.242),
    ("26", "PE", "Pernambuco", "Nordeste", 9058931, 98067.877),
    ("27", "AL", "Alagoas", "Nordeste", 3127683, 27830.657),
    ("28", "SE", "Sergipe", "Nordeste", 2210004, 21925.259),
    ("29", "BA", "Bahia", "Nordeste", 14141626, 564760.429),
    ("31", "MG", "Minas Gerais", "Sudeste", 20539989, 586513.983),
    ("32", "ES", "Espírito Santo", "Sudeste", 3833712, 46074.448),
    ("33", "RJ", "Rio de Janeiro", "Sudeste", 16054524, 43750.425),
    ("35", "SP", "São Paulo", "Sudeste", 44411238, 248219.485),
    ("41", "PR", "Paraná", "Sul", 11444380, 199307.945),
    ("42", "SC", "Santa Catarina", "Sul", 7610361, 95730.690),
    ("43", "RS", "Rio Grande do Sul", "Sul", 10882965, 281707.151),
    ("50", "MS", "Mato Grosso do Sul", "Centro-Oeste", 2757013, 357144.984),
    ("51", "MT", "Mato Grosso", "Centro-Oeste", 3567234, 903207.751),
    ("52", "GO", "Goiás", "Centro-Oeste", 7113540, 340242.909),
    ("53", "DF", "Distrito Federal", "Centro-Oeste", 2817381, 5760.783),
]

# Fontes primárias do BEN reduzidas a 8 categorias (ver transform_matriz_energetica.py)
FONTES_ENERGIA = [
    ("PETROLEO", "Petróleo", "Não renovável"),
    ("GAS_NATURAL", "Gás Natural", "Não renovável"),
    ("CARVAO", "Carvão Mineral", "Não renovável"),
    ("URANIO", "Urânio (Nuclear)", "Não renovável"),
    ("HIDRAULICA", "Energia Hidráulica", "Renovável"),
    ("LENHA", "Lenha", "Renovável"),
    ("CANA", "Produtos da Cana", "Renovável"),
    ("OUTRAS_PRIM", "Outras Primárias (solar/eólica)", "Renovável"),
]


def gerar_dim_tempo() -> pd.DataFrame:
    linhas = []
    for ano in range(ANO_INICIO, ANO_FIM + 1):
        for mes in range(1, 13):
            id_tempo = ano * 100 + mes
            trimestre = (mes - 1) // 3 + 1
            linhas.append({"id_tempo": id_tempo, "ano": ano, "mes": mes, "trimestre": trimestre})
    return pd.DataFrame(linhas)


def carregar_pib_mais_recente() -> dict:
    """Lê o JSON mais recente de PIB por UF e retorna {cod_ibge: pib_total_mil_reais} do último ano disponível."""
    arquivos = sorted(glob.glob(str(RAW_IBGE_DIR / "ibge_pib_uf_*.json")))
    if not arquivos:
        print("[seed_dimensoes] aviso: nenhum arquivo de PIB do IBGE encontrado — "
              "pib_per_capita ficará nulo. Rode extract_ibge_contas_regionais.py primeiro.", file=sys.stderr)
        return {}

    with open(arquivos[-1], encoding="utf-8") as f:
        dados = json.load(f)[1:]  # [0] é o cabeçalho da SIDRA

    df = pd.DataFrame(dados)
    ultimo_ano = df["D3C"].astype(int).max()
    df_ultimo = df[df["D3C"].astype(int) == ultimo_ano]
    print(f"[seed_dimensoes] usando PIB do ano {ultimo_ano} (mais recente disponível na base)")
    return dict(zip(df_ultimo["D1C"], df_ultimo["V"].astype(float)))


def gerar_dim_uf() -> pd.DataFrame:
    pib_por_ibge = carregar_pib_mais_recente()

    linhas = []
    for cod_ibge, cod_uf, nome_uf, regiao, populacao, area_km2 in UF_REFERENCIA:
        pib_total_mil_reais = pib_por_ibge.get(cod_ibge)
        if pib_total_mil_reais is not None and populacao:
            # V vem em "Mil Reais" (MN da SIDRA) -> multiplica por 1000 para reais, depois divide pela população
            pib_per_capita = round((pib_total_mil_reais * 1000) / populacao, 2)
            pib_projetado = False
        else:
            pib_per_capita = None
            pib_projetado = True
        linhas.append({
            "cod_uf": cod_uf,
            "nome_uf": nome_uf,
            "regiao": regiao,
            "pib_per_capita": pib_per_capita,
            "pib_projetado": pib_projetado,
            "populacao": populacao,
            "area_km2": area_km2,
        })
    return pd.DataFrame(linhas)


def gerar_dim_fonte_energia() -> pd.DataFrame:
    return pd.DataFrame(FONTES_ENERGIA, columns=["cod_fonte", "nome_fonte", "categoria"])


if __name__ == "__main__":
    TRUSTED_DIR.mkdir(parents=True, exist_ok=True)

    dim_tempo = gerar_dim_tempo()
    dim_tempo.to_csv(TRUSTED_DIR / "dim_tempo.csv", index=False)
    print(f"[seed_dimensoes] dim_tempo: {len(dim_tempo)} linhas salvas")

    dim_uf = gerar_dim_uf()
    dim_uf.to_csv(TRUSTED_DIR / "dim_uf.csv", index=False)
    print(f"[seed_dimensoes] dim_uf: {len(dim_uf)} linhas salvas")

    dim_fonte = gerar_dim_fonte_energia()
    dim_fonte.to_csv(TRUSTED_DIR / "dim_fonte_energia.csv", index=False)
    print(f"[seed_dimensoes] dim_fonte_energia: {len(dim_fonte)} linhas salvas")

    print("[seed_dimensoes] concluído.")
