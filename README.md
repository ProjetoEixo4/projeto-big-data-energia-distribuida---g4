# Geração de Energia Distribuída no Brasil — Pipeline de Integração de Dados

Projeto de Big Data Analytics (PUC Minas, Eixo 4) — Etapa 2: Replicação e Integração de Dados.

Observatório de Energia e Transição Digital (OET) — estudo comparativo independente sobre
fontes de energia, custos e economia da geração distribuída para consumidores unifamiliares,
multifamiliares e rurais no Brasil.

## Visão geral da arquitetura

```
Fontes públicas (ANEEL, EPE, INMET, IBGE)
        │  extração (boto3 / requests / API SIDRA)
        ▼
   S3 — camada raw           (dados originais, particionados por fonte/data)
        │  AWS Glue (PySpark / Python Shell)
        ▼
   S3 — camada trusted       (Parquet particionado por ano/UF)
        │  carga (COPY / psycopg2, idempotente via staging + upsert)
        ▼
   Amazon RDS (PostgreSQL)   (modelo em constelação de fatos)
        │
        ▼
   Athena (consultas ad hoc sobre a camada trusted) + QuickSight (dashboards)
```

Orquestração: Amazon EventBridge (agendamento) + AWS Step Functions (sequenciamento dos jobs).
Monitoramento: Amazon CloudWatch (logs e alarmes de falha).

Ver `docs/architecture.png` para o diagrama completo e `sql/` para o modelo dimensional.

## Recorte temporal do projeto

Em conversa com o orientador, o grupo definiu delimitar o projeto ao período de
**2020 a 2025**, usando cinco fontes: ANEEL (Geração Distribuída e Tarifas), EPE
(BEN), INMET (BDMEP) e IBGE (PIB dos Municípios/SIDRA). O filtro temporal é
aplicado tanto na extração (quando a fonte permite, como INMET e IBGE) quanto na
transformação (como camada de segurança para todas as fontes — ver
`etl/transform_common.py`).

**Duas ressalvas de cobertura conhecidas, não são erros de pipeline:**

- **IBGE — PIB dos Municípios**: defasagem de divulgação de ~2 anos. A edição
  mais recente (dez/2025) cobre até 2023 — 2024 e 2025 ficarão sem esse
  indicador até novas divulgações do IBGE.
- **EPE — BEN**: publicado com defasagem de 1 ano em relação ao ano-base. A
  edição com dados completos de 2025 só estará disponível em 2026.

## Nota sobre ABSOLAR e Greener

Essas duas fontes foram avaliadas na Etapa 1 como possíveis fontes complementares de
custo de instalação (preço por watt-pico), mas **excluídas do pipeline automatizado**:

- **ABSOLAR** exige cadastro prévio para acesso à base — solicitado pelo grupo, sem resposta.
- **Greener** não publica seus estudos em formato aberto e estruturado.

A dimensão de custo é tratada, em vez disso, por (i) um indicador indireto calculado a
partir da própria ANEEL (taxa de crescimento da potência instalada, como proxy de
maturidade de mercado) e (ii) citação pontual de literatura secundária que já reproduz
números da ABSOLAR (ex.: imprensa especializada, publicações do BNDES), sem necessidade
de acesso direto à fonte primária. A tabela `oet.apoio_custo_instalacao` (ver
`sql/02_facts.sql`) é mantida no modelo apenas como registro de referência desses valores
citados — não há script de extração associado a ela.

## Estrutura do repositório

```
sql/            DDL do banco (dimensões e fatos) — PostgreSQL
etl/            Scripts Python de extração, transformação e carga
glue/           Job PySpark para o AWS Glue (fonte de maior volume: ANEEL)
infra/          Comandos AWS CLI e política IAM mínima
orchestration/  Definição do Step Functions (State Machine)
docs/           Diagrama de arquitetura
```

## Como rodar localmente (antes de subir para a AWS)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 1. Extração (grava em ./data/raw/<fonte>/) — 5 fontes automatizadas
python etl/extract_aneel_gd.py
python etl/extract_aneel_tarifas.py
python etl/extract_epe_ben.py
python etl/extract_ibge_sidra.py
python etl/extract_inmet.py
# ABSOLAR e Greener não têm script de extração — ver seção "Nota sobre ABSOLAR e Greener" acima

# 2. Transformação (grava em ./data/trusted/ como Parquet particionado)
python etl/transform_common.py

# 3. Carga no PostgreSQL (local ou RDS, via variável de ambiente DATABASE_URL)
export DATABASE_URL="postgresql://usuario:senha@localhost:5432/oet"
psql "$DATABASE_URL" -f sql/01_dimensions.sql
psql "$DATABASE_URL" -f sql/02_facts.sql
python etl/load_postgres.py
```

## Como migrar para a AWS

1. Criar os buckets S3 (`infra/create_s3_buckets.sh`).
2. Criar um RDS PostgreSQL (Free Tier: `db.t3.micro`, 20 GB).
3. Publicar `glue/glue_job_aneel_gd.py` como um Glue Job (Python Shell ou Spark, conforme
   volume real observado na extração).
4. Publicar a State Machine de `orchestration/step_functions_definition.json`.
5. Agendar via EventBridge (ex.: diariamente às 03h).

## Custos estimados (uso acadêmico, dentro do Free Tier na maior parte)

| Serviço | Estimativa mensal |
|---|---|
| S3 (< 5 GB) | Gratuito (Free Tier) |
| RDS PostgreSQL `db.t3.micro` | Gratuito por 12 meses (Free Tier) |
| AWS Glue | ~US$ 0,44/hora de DPU processada (uso pontual, poucas horas/mês) |
| Athena | ~US$ 5 por TB escaneado (volumes do projeto: centavos/mês) |
| Step Functions + EventBridge | Gratuito na faixa de uso do projeto |
| QuickSight | 1 usuário grátis por 30 dias (Standard); após isso, ~US$ 9/usuário/mês |

## Links

- Repositório: `[PREENCHER — link do GitHub do grupo após o push]`
- Documento completo da Etapa 2 (PDF/DOCX): ver entrega no AVA.
