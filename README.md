# Observatório de Energia e Transição Digital (OET) — Grupo 3, Eixo 4

Pipeline de Big Data Analytics sobre a **expansão da geração distribuída solar no Brasil (2020–2025)**,
em granularidade UF × mês, cruzando regulação (ANEEL), clima (INMET), preços (ANEEL + IPCA), socioeconomia
(IBGE) e a matriz elétrica nacional (EPE/BEN).

Curso de Tecnologia em Banco de Dados — PUC Minas. Integrantes: Fernanda Coimbra, Giovanna Bruniera de Oliveira,
Guilherme Henrique Vicente Oliveira, Lucas Lima de Paula, Mariana Reis Mendes, Vivian Rebelo Assunção.

## Arquitetura (Etapa 3)

```
EventBridge (agenda mensal) ─▶ Step Functions ─────────────────────────────────────────────┐
                                  │                                                         │ falha (após Retry)
   ┌── Extração (6 em paralelo) ──┤                                                         ▼
   │   ANEEL GD · ANEEL GD técnico · ANEEL tarifas · INMET · IBGE PIB · IBGE IPCA          SNS ─▶ e-mail
   ▼                                                                                         ▲
 S3 RAW ─▶ Dimensões ─▶ Transformação (3 em paralelo) ─▶ Tarifa ─▶ Carga ─▶ SNS (sucesso) ───┘
 (como veio)            GD em PySpark · Clima · Matriz        │       │
                                                              ▼       ▼
                                                      S3 TRUSTED   RDS PostgreSQL
                                                      (Parquet)    (staging + upsert, atômico)
```

* **Glue Python Shell** (`glue/run_step.py` + `glue/runner.py`): um runner único executa os mesmos scripts
  `etl/` e `transform/` que rodam no computador — o que se testa localmente é o que vai para a nuvem.
* **Glue Spark** (`glue/glue_job_gd_pyspark.py`): geração distribuída (4,6 milhões de usinas) em PySpark; equivalência
  com a versão pandas comprovada por `tests/test_equivalencia_spark.py`.
* **Step Functions** (`orchestration/`): paralelismo, `Retry` com backoff, `Catch` → SNS, simulação de falha.
* **Carga idempotente** (`etl/load_postgres.py`): valida → staging (COPY) → upsert só do que mudou → auditoria em
  `etl_execucao`; tudo numa transação. Reexecutar não duplica nem altera nada.
* **Infraestrutura como código** (`infra/oet-pipeline.yaml`): buckets, IAM de menor privilégio, Secrets Manager,
  rede privada com NAT para o job de carga, Glue, Step Functions, EventBridge, SNS.

## Modelo dimensional (constelação de fatos, UF × mês)

| Tabela | Grão | Observação |
|---|---|---|
| `fato_geracao_uf` | mês × UF × modalidade | **estoque acumulado** de potência/usinas solares + fluxo de novas conexões |
| `fato_tarifa` | mês × UF | R$/kWh nominal e em **reais de dez/2025** (IPCA) |
| `fato_matriz_energetica` | ano (nacional) × fonte | geração elétrica em GWh: hidráulica, eólica, **solar**, térmica/outras |
| `dim_clima_uf` | mês × UF | irradiação **kWh/m²/dia**, temperatura, chuva, cobertura da amostra |
| `dim_tempo`, `dim_uf`, `dim_distribuidora`, `dim_modalidade`, `dim_fonte_energia` | — | `dim_modalidade` tem catálogo fixo de 50 linhas com id estável |

## Decisões de dados (o que foi corrigido da Etapa 2)

* **Tarifa**: o arquivo da ANEEL tem ~328 mil linhas para ~2 mil tarifas relevantes. O recorte é B1 residencial,
  convencional, *Tarifa de Aplicação*; cada reajuste vale até o próximo; média mensal ponderada por dias; UF =
  média das distribuidoras ponderada pelas usinas solares conectadas. `tarifa_deflacionada` é de fato deflacionada.
* **Geração**: só solar (UFV, 99,98 % do cadastro); série mensal **acumulada** a partir da data de conexão
  (`DatConexao`, tabela técnica da ANEEL). Sem esse arquivo o pipeline cai para a data de atualização cadastral e
  marca `serie_aproximada = true` no relatório de qualidade.
* **Clima**: a expressão regular antiga descartava as regiões de 1 letra (N, S) e a pasta `2025/` — saíam 1.020 linhas
  em vez de 1.944 (27 UFs × 72 meses). Radiação passou de kJ/m² médio para **kWh/m²/dia**, com validade diurna e
  `cobertura_pct`.
* **Matriz**: o extrato antigo media energia primária e não isolava a solar; agora usa a matriz em unidades comerciais.
* **PIB**: IBGE publica com ~2 anos de defasagem; `dim_uf.ano_ref_pib` registra o ano usado (2023).

## Executar localmente

```bash
python -m venv .venv && source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                                      # preencha as credenciais do RDS

python etl/extract_aneel_gd.py
python etl/extract_aneel_gd_tecnico.py
python etl/extract_aneel_tarifas.py
python etl/extract_inmet.py --ano-inicio 2020 --ano-fim 2025
python etl/extract_ibge_contas_regionais.py
python etl/extract_ipca.py
python etl/extract_epe_ben.py --pasta "<pasta com matriz_balanco_energitico_unidades_comerciais_AAAA.xlsx>"

python transform/seed_dimensoes.py
python transform/transform_geracao_uf.py
python transform/transform_clima_uf.py
python transform/transform_tarifa.py          # depende da geração (pesos por distribuidora) e do IPCA
python transform/transform_matriz_energetica.py
python etl/load_postgres.py --ddl             # cria/migra o esquema e carrega
```

Relatórios de qualidade de cada transformação ficam em `data/trusted/_qualidade_*.json`.

## Na AWS

Passo a passo completo em [`docs/GUIA_IMPLANTACAO_ETAPA3.md`](docs/GUIA_IMPLANTACAO_ETAPA3.md).
Resumo: `python infra/deploy_aws.py implantar --email ...` → `artefatos` → `executar`.

## Testes

```bash
pip install -r requirements-dev.txt
python -m pytest tests/test_unitarios.py -q           # lógica de negócio, segundos, sem rede
python tests/test_equivalencia_spark.py               # pandas == Spark (dados sintéticos e reais)
python tests/test_pipeline_e2e_moto.py                # pipeline inteiro em S3 simulado + PostgreSQL de teste
cfn-lint infra/oet-pipeline.yaml
```

## Limitações conhecidas

* Sem `DatConexao` a série de geração é aproximada (o relatório de qualidade avisa).
* Térmica/outras é resíduo (o arquivo da EPE não separa gás, carvão, nuclear e biomassa em GWh).
* PIB disponível só até 2023; tarifa por UF usa as usinas solares como proxy de peso de consumidores.
* O job de carga usa NAT Gateway (~US$ 1,5/dia): remova a stack após as entregas (`deploy_aws.py destruir`).
