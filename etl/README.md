# Scripts de extração — Etapa 2

Cinco scripts, um por fonte de dados. Todos seguem o mesmo padrão:
argumentos de linha de comando, download ou consulta à API, gravação em
`data/raw/<fonte>/`, log de progresso no terminal.

## Como rodar cada um

```bash
# ANEEL — Geração Distribuída (arquivo Parquet nacional)
python etl/extract_aneel_gd.py

# ANEEL — Tarifas de Energia (CSV)
python etl/extract_aneel_tarifas.py

# EPE — Balanço Energético Nacional (requer download manual prévio do dashboard)
python etl/extract_epe_ben.py --arquivo ~/Downloads/ben_anexo_2025.xlsx --ano-base 2025

# INMET — BDMEP (ZIPs anuais, todas as estações)
python etl/extract_inmet.py --ano-inicio 2020 --ano-fim 2025

# IBGE — Contas Regionais (PIB por UF, via API SIDRA)
python etl/extract_ibge_contas_regionais.py --anos 2020 2021 2022 2023
```

## Rodar todos de uma vez

```bash
python etl/extract_aneel_gd.py && \
python etl/extract_aneel_tarifas.py && \
python etl/extract_inmet.py --ano-inicio 2020 --ano-fim 2025 && \
python etl/extract_ibge_contas_regionais.py --anos 2020 2021 2022 2023
echo "Não esqueça do EPE BEN — download manual pelo dashboard antes de organizar o arquivo."
```

## Por que a extração não filtra nem transforma nada

Cada script grava o dado exatamente como veio da fonte, na camada raw.
O filtro temporal (2020–2025), a agregação por UF, a classificação de
consumidor e a deflação monetária acontecem todos na fase de
**transformação** (`etl/transform_*.py`), não aqui. Isso significa que,
se algum critério de filtro mudar no futuro, você não precisa baixar os
dados de novo — só reprocessar a partir do que já está na camada raw.

## Dependências

```bash
pip install requests pandas pyarrow
```
