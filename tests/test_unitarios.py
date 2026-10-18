"""Testes unitários com dados sintéticos pequenos (rodam em segundos, sem rede e sem dados reais).

    python -m pytest tests/test_unitarios.py -q
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from etl import load_postgres as carga
from transform import seed_dimensoes, transform_clima_uf as clima, transform_geracao_uf as ger, transform_tarifa as tar
from transform import transform_matriz_energetica as matriz

FIXTURE_IPCA = "tests/fixtures/ipca_sidra_1737_201912_202512.json"


# ----------------------------------------------------------------------------- clima
@pytest.mark.parametrize("nome,esperado", [
    ("INMET_CO_DF_A001_BRASILIA_01-01-2020_A_31-12-2020.CSV", "DF"),
    ("INMET_S_RS_A801_PORTO ALEGRE_01-01-2022_A_31-12-2022.CSV", "RS"),      # região de 1 letra (bug antigo)
    ("INMET_N_AM_A101_MANAUS_01-01-2022_A_31-12-2022.CSV", "AM"),
    ("2025/INMET_NE_BA_A401_SALVADOR_01-01-2025_A_31-12-2025.CSV", "BA"),    # pasta 2025/ (bug antigo)
    ("INMET_SE_XX_A001_FAKE_01-01-2020_A_31-12-2020.CSV", None),             # UF inexistente
    ("LEIAME.txt", None),
])
def test_uf_do_arquivo(nome, esperado):
    assert clima.uf_do_arquivo(nome) == esperado


def _csv_estacao(dias: int, kj_por_hora_diurna: float, horas_diurnas: int = 13) -> bytes:
    cab = "\n".join([f"META{i};x" for i in range(8)])
    linhas = [f"Data;Hora UTC;{clima.COL_PRECIPITACAO};{clima.COL_RADIACAO};{clima.COL_TEMPERATURA};"]
    for d in range(1, dias + 1):
        for h in range(24):
            diurna = clima.JANELA_DIURNA_UTC[0] <= h <= clima.JANELA_DIURNA_UTC[1]
            rad = f"{kj_por_hora_diurna:.1f}".replace(".", ",") if diurna else ""   # noite em branco, como no INMET
            linhas.append(f"2022/01/{d:02d};{h:02d}00 UTC;0,2;{rad};25,0;")
    return (cab + "\n" + "\n".join(linhas)).encode("latin1")


def test_irradiacao_kwh_m2_dia():
    # 13 horas diurnas x 1000 kJ/m² = 13000 kJ = 3,611 kWh/m²/dia
    df = clima.processar_estacao_bytes(_csv_estacao(31, 1000.0), "SP")
    assert df is not None and len(df) == 1
    assert df["irradiacao_kwh_m2_dia"].iloc[0] == pytest.approx(13000 / 3600, rel=1e-6)
    assert df["cobertura_pct"].iloc[0] == pytest.approx(100.0)
    assert df["precipitacao_total_mm"].iloc[0] == pytest.approx(0.2 * 24 * 31)


def test_irradiacao_absurda_vira_nulo():
    # 13 x 3000 kJ = 10,8 kWh/m²/dia > MAX_KWH_DIA -> erro de sensor, dia descartado
    df = clima.processar_estacao_bytes(_csv_estacao(5, 3000.0), "SP")
    assert np.isnan(df["irradiacao_kwh_m2_dia"].iloc[0])


# ----------------------------------------------------------------------------- tarifa
def _linhas_tarifa(cnpj, inicio, tusd, te):
    return {"NumCNPJDistribuidora": cnpj, "DatInicioVigencia": inicio, "DscSubGrupo": "B1", "DscClasse": "Residencial",
            "DscSubClasse": "Residencial", "DscModalidadeTarifaria": "Convencional",
            "DscBaseTarifaria": "Tarifa de Aplicação", "DscDetalhe": "Não se aplica",
            "NomPostoTarifario": "Não se aplica", "VlrTUSD": tusd, "VlrTE": te}


def test_filtro_tarifa_exclui_variantes():
    base = _linhas_tarifa("1", "2020-01-01", "300", "200")
    ruins = [{**base, "DscSubClasse": "Baixa Renda"}, {**base, "DscModalidadeTarifaria": "Branca"},
             {**base, "DscBaseTarifaria": "Base Econômica"}, {**base, "DscDetalhe": "SCEE"},
             {**base, "NomPostoTarifario": "Ponta"}, {**base, "DscSubGrupo": "A4"}]
    df = pd.DataFrame([base] + ruins)
    assert len(tar.filtrar_b1_residencial(df)) == 1


def test_reajuste_no_meio_do_mes_pondera_por_dias():
    # 0,50 R$/kWh até 20/jan; 0,60 a partir de 21/jan -> janeiro: (20*0,5 + 11*0,6)/31
    df = pd.DataFrame([_linhas_tarifa("1", "2019-06-01", "300", "200"), _linhas_tarifa("1", "2020-01-21", "350", "250")])
    m = tar.tarifa_mensal_por_distribuidora(df).set_index("id_tempo")["tarifa"]
    assert m[202001] == pytest.approx((20 * 0.5 + 11 * 0.6) / 31)
    assert m[202002] == pytest.approx(0.6) and m[202512] == pytest.approx(0.6)   # vale o último reajuste


def test_agregacao_uf_pondera_por_usinas():
    mensal = pd.DataFrame({"id_tempo": [202001, 202001], "cod_distribuidora": ["A", "B"], "tarifa": [0.5, 1.0]})
    pesos = pd.DataFrame({"cod_distribuidora": ["A", "B"], "cod_uf": ["MG", "MG"], "qtd_usinas": [3, 1]})
    r = tar.agregar_por_uf(mensal, pesos)
    assert r["tarifa_media_reais_kwh"].iloc[0] == pytest.approx((0.5 * 3 + 1.0 * 1) / 4)


def test_deflacao_traz_para_reais_de_dez_2025():
    ipca = tar.carregar_ipca(__import__("pathlib").Path(FIXTURE_IPCA))
    fato = pd.DataFrame({"id_tempo": [202001, 202512], "cod_uf": ["MG", "MG"], "tarifa_media_reais_kwh": [0.50, 0.50]})
    r = tar.deflacionar(fato, ipca)
    assert r["tarifa_deflacionada"].iloc[1] == pytest.approx(0.50)                 # último mês: igual
    assert r["tarifa_deflacionada"].iloc[0] > 0.50 * 1.30                          # 2020 -> 2025: inflação acumulada > 30 %
    with pytest.raises(RuntimeError):
        tar.deflacionar(fato.assign(id_tempo=[201001, 202512]), ipca)             # mês sem índice: falha explícita


# ----------------------------------------------------------------------------- geração
def _usinas(linhas):
    cols = ["CodEmpreendimento", "SigUF", "DscModalidadeHabilitado", "DscClasseConsumo",
            "MdaPotenciaInstaladaKW", "DthAtualizaCadastralEmpreend"]
    return pd.DataFrame(linhas, columns=cols)


def test_estoque_acumulado_saldo_inicial_e_fluxo(monkeypatch):
    dim = seed_dimensoes.gerar_dim_modalidade()
    monkeypatch.setattr(ger, "ler_trusted", lambda nome: dim)
    mod, cls = "Geração na própria UC", "Residencial"
    df = _usinas([
        ("A", "MG", mod, cls, 5.0, "2019-12-15"),    # antes de 2020 -> saldo inicial
        ("B", "MG", mod, cls, 3.0, "2020-03-10"),
        ("C", "MG", mod, cls, 2.0, "2020-03-20"),
        ("D", "MG", mod, cls, 4.0, "2021-01-05"),
        ("E", "MG", mod, cls, 9.0, "1900-01-01"),    # data inválida -> descartada
    ])
    df["NumCNPJDistribuidora"] = 1
    com_data, resumo = ger.definir_mes_de_entrada(df, None)
    assert len(com_data) == 4 and resumo["serie_aproximada"] is True
    fato = ger.construir_fato(com_data).set_index("id_tempo")
    assert fato.loc[202001, "potencia_acumulada_kw"] == 5.0 and fato.loc[202001, "qtd_acumulada"] == 1
    assert fato.loc[202003, "potencia_nova_kw"] == 5.0 and fato.loc[202003, "qtd_novas_conexoes"] == 2
    assert fato.loc[202003, "potencia_acumulada_kw"] == 10.0
    assert fato.loc[202004, "potencia_nova_kw"] == 0 and fato.loc[202004, "potencia_acumulada_kw"] == 10.0   # carrega adiante
    assert fato.loc[202512, "potencia_acumulada_kw"] == 14.0 and fato.loc[202512, "qtd_acumulada"] == 4
    assert len(fato) == 72                                                                                    # grade completa


def test_data_de_conexao_tem_prioridade_sobre_cadastro():
    df = _usinas([("A", "MG", "x", "y", 1.0, "2024-05-05"), ("B", "MG", "x", "y", 1.0, "2024-05-05")])
    tecnico = pd.DataFrame({"CodEmpreendimento": ["A"], "DatConexao": pd.to_datetime(["2021-02-01"])})
    r, resumo = ger.definir_mes_de_entrada(df, tecnico)
    por = r.set_index("CodEmpreendimento")
    assert str(por.loc["A", "mes_entrada"]) == "2021-02" and por.loc["A", "origem_data"] == "conexao"
    assert str(por.loc["B", "mes_entrada"]) == "2024-05" and por.loc["B", "origem_data"] == "cadastro"
    assert resumo["pct_usinas_data_conexao"] == 50.0


# ----------------------------------------------------------------------------- dimensões e carga
def test_dim_modalidade_estavel_e_completa():
    d = seed_dimensoes.gerar_dim_modalidade()
    assert len(d) == 50 and d["id_modalidade"].tolist() == list(range(1, 51))
    assert d.loc[(d.modalidade_aneel == "Condomínio") & (d.classe_consumo == "Residencial"), "tipo_consumidor"].iloc[0] == "Multifamiliar"
    assert d.loc[(d.modalidade_aneel == "Geração na própria UC") & (d.classe_consumo == "Rural"), "tipo_consumidor"].iloc[0] == "Rural"


def test_validacao_da_carga_detecta_problemas():
    uf = pd.DataFrame({"cod_uf": ["MG", "SP"], "nome_uf": ["a", "b"], "regiao": ["x", "y"], "pib_per_capita": [1, 2],
                       "pib_projetado": [False, False], "ano_ref_pib": [2023, 2023], "populacao": [1, 2], "area_km2": [1, 2]})
    carga.validar({"dim_uf": uf})                                   # ok
    with pytest.raises(carga.ErroValidacao, match="duplicada"):
        carga.validar({"dim_uf": pd.concat([uf, uf])})
    tempo = pd.DataFrame({"id_tempo": [202001], "ano": [2020], "mes": [1], "trimestre": [1]})
    tarifa = pd.DataFrame({"id_tempo": [202001, 209912], "cod_uf": ["MG", "SP"], "tarifa_media_reais_kwh": [1, 1],
                           "tarifa_deflacionada": [1, 1], "qtd_distribuidoras": [1, 1]})
    with pytest.raises(carga.ErroValidacao, match="sem pai"):
        carga.validar({"dim_uf": uf, "dim_tempo": tempo, "fato_tarifa": tarifa})
    with pytest.raises(carga.ErroValidacao, match="vazio"):
        carga.validar({"dim_uf": uf.iloc[0:0]})


# ----------------------------------------------------------------------------- matriz elétrica (BEN)
def test_matriz_eletrica_decomposicao(tmp_path):
    cols = ["grupo", "Hidráulica - GWh", "Eólica - GWh", "Solar Fotovoltaica - GWh", "Eletricidade - GWh"]
    d = pd.DataFrame([
        ["Produção", 600.0, 200.0, 100.0, 0.0],
        [matriz.CENTRAL_PUBLICA, -550.0, -190.0, -40.0, 800.0],
        [matriz.CENTRAL_AUTOPROD, -50.0, -10.0, -60.0, 200.0],
    ], columns=cols)
    arq = tmp_path / "ben_comerciais_2024.xlsx"
    d.to_excel(arq, index=False)
    linhas = {l["cod_fonte"]: l for l in matriz.processar_arquivo(arq, 2024)}
    assert linhas["SOLAR_FV"]["geracao_gwh"] == 100.0 and linhas["SOLAR_FV"]["geracao_autoprodutoras_gwh"] == 60.0
    assert linhas["TERMICA_OUTRAS"]["geracao_gwh"] == 1000.0 - 900.0           # resíduo: total - hid - eol - sol
    assert linhas["TERMICA_OUTRAS"]["geracao_servico_publico_gwh"] == 800.0 - 780.0
    assert sum(l["participacao_percentual"] for l in linhas.values()) == pytest.approx(100.0, abs=0.05)
