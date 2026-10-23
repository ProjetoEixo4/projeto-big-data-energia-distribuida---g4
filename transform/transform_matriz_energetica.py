"""
Transformação — EPE BEN → fato_matriz_energetica (geração ELÉTRICA nacional por fonte)

Pergunta que essa tabela responde: qual a participação da energia solar na
eletricidade gerada no Brasil, ano a ano, frente às demais fontes? Para isso
usamos a matriz "em unidades comerciais" do BEN, que traz em GWh a produção de
cada fonte elétrica e a coluna de eletricidade gerada pelas centrais elétricas:

    Hidráulica / Eólica / Solar fotovoltaica  = linha "Produção" das colunas
                                                 "<fonte> - GWh" (o BEN já
                                                 contabiliza essas fontes a
                                                 100% na transformação)
    Total de eletricidade gerada              = Centrais Elétricas de Serviço
                                                 Público + Centrais Elétricas
                                                 Autoprodutoras (coluna
                                                 "Eletricidade - GWh")
    Térmica e outras                          = total − hidráulica − eólica
                                                 − solar (gás, carvão,
                                                 nuclear, biomassa,
                                                 derivados: o arquivo não
                                                 separa isso em GWh, então
                                                 vira resíduo)

A divisão por tipo de central fica preservada: autoprodutoras concentram a
geração distribuída (consumidores que geram a própria energia), enquanto
serviço público são as usinas despachadas/centralizadas. É um indicador
NACIONAL e ANUAL — não tem UF —, com id_tempo = AAAA12 (dezembro do ano-base),
seguindo a mesma convenção da dimensão tempo mensal.

Uso:
    python transform/transform_matriz_energetica.py
Gera (Parquet, camada trusted): fato_matriz_energetica
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from oet.io import raw_dir, salvar_relatorio, salvar_trusted  # noqa: E402

RAW_DIR = raw_dir("epe_ben")
ARQUIVO_RE = re.compile(r"ben_comerciais_(\d{4})\.xlsx$")

CENTRAL_PUBLICA = "Centrais Elétricas de Serviço Público"
CENTRAL_AUTOPROD = "Centrais Elétricas Autoprodutoras"
COL_ELETRICIDADE = "Eletricidade - GWh"
FONTES_DIRETAS = {  # cod_fonte -> coluna da matriz
    "HIDRAULICA": "Hidráulica - GWh",
    "EOLICA": "Eólica - GWh",
    "SOLAR_FV": "Solar Fotovoltaica - GWh",
}


def processar_arquivo(caminho: Path, ano: int) -> list[dict]:
    d = pd.read_excel(caminho, sheet_name=0).set_index("grupo")
    faltam = [c for c in [COL_ELETRICIDADE, *FONTES_DIRETAS.values()] if c not in d.columns]
    if faltam:
        raise RuntimeError(f"{caminho.name}: colunas ausentes {faltam}")

    pub = float(d.loc[CENTRAL_PUBLICA, COL_ELETRICIDADE])
    aut = float(d.loc[CENTRAL_AUTOPROD, COL_ELETRICIDADE])
    total, total_pub, total_aut = pub + aut, pub, aut

    linhas, soma_pub, soma_aut = [], 0.0, 0.0
    for cod, coluna in FONTES_DIRETAS.items():
        # nas linhas das centrais o BEN registra o INSUMO consumido (negativo); como a
        # transformação é 1:1 para estas fontes, o módulo é a eletricidade entregue
        g_pub = -float(d.loc[CENTRAL_PUBLICA, coluna])
        g_aut = -float(d.loc[CENTRAL_AUTOPROD, coluna])
        prod = float(d.loc["Produção", coluna])
        linhas.append((cod, prod, g_pub, g_aut))
        soma_pub += g_pub
        soma_aut += g_aut
    ger_dir = sum(x[1] for x in linhas)
    linhas.append(("TERMICA_OUTRAS", total - ger_dir, total_pub - soma_pub, total_aut - soma_aut))

    return [{
        "id_tempo": ano * 100 + 12, "cod_fonte": cod,
        "participacao_percentual": round(100 * g / total, 2),
        "geracao_gwh": round(g, 1),
        "geracao_servico_publico_gwh": round(gp, 1),
        "geracao_autoprodutoras_gwh": round(ga, 1),
    } for cod, g, gp, ga in linhas]


def transformar() -> pd.DataFrame:
    arquivos = sorted(p for p in RAW_DIR.glob("ben_comerciais_*.xlsx"))
    if not arquivos:
        raise FileNotFoundError(f"nenhum ben_comerciais_AAAA.xlsx em {RAW_DIR} — rode etl/extract_epe_ben.py --pasta ...")
    linhas = []
    for arq in arquivos:
        ano = int(ARQUIVO_RE.search(arq.name).group(1))
        print(f"[transform_matriz_energetica] {arq.name} (ano {ano})")
        linhas.extend(processar_arquivo(arq, ano))
    fato = pd.DataFrame(linhas).sort_values(["id_tempo", "cod_fonte"]).reset_index(drop=True)

    # coerência: as fontes de cada ano somam 100 %
    soma = fato.groupby("id_tempo")["participacao_percentual"].sum()
    if not ((soma - 100).abs() < 0.1).all():
        raise RuntimeError(f"participações não somam 100 %: {soma.to_dict()}")
    if (fato["geracao_gwh"] < 0).any():
        raise RuntimeError("geração negativa após a decomposição — verifique o arquivo do BEN")

    salvar_trusted(fato, "fato_matriz_energetica")
    solar = fato[fato["cod_fonte"] == "SOLAR_FV"].set_index("id_tempo")["participacao_percentual"]
    salvar_relatorio("matriz", {"linhas": int(len(fato)), "anos": sorted({int(t // 100) for t in fato["id_tempo"]}),
                                "participacao_solar_pct": {str(k // 100): float(v) for k, v in solar.items()}})
    print(f"[transform_matriz_energetica] fato_matriz_energetica: {len(fato)} linhas; "
          f"solar % = {solar.to_dict()}")
    return fato


if __name__ == "__main__":
    try:
        transformar()
    except (FileNotFoundError, RuntimeError) as e:
        print(f"[transform_matriz_energetica] ERRO: {e}", file=sys.stderr)
        sys.exit(1)
