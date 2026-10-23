
-- Modelo Dimensional — Constelação de Fatos (granularidade UF x mês)



CREATE TABLE IF NOT EXISTS dim_tempo (
    id_tempo    INTEGER PRIMARY KEY,                 -- AAAAMM
    ano         SMALLINT NOT NULL,
    mes         SMALLINT NOT NULL CHECK (mes BETWEEN 1 AND 12),
    trimestre   SMALLINT NOT NULL CHECK (trimestre BETWEEN 1 AND 4)
);

CREATE TABLE IF NOT EXISTS dim_uf (
    cod_uf          CHAR(2) PRIMARY KEY,
    nome_uf         VARCHAR(40) NOT NULL,
    regiao          VARCHAR(20) NOT NULL,
    pib_per_capita  NUMERIC(12,2),
    pib_projetado   BOOLEAN NOT NULL DEFAULT FALSE,  -- FALSE = valor observado em ano_ref_pib
    ano_ref_pib     SMALLINT,                        -- ano do PIB (IBGE publica com ~2 anos de defasagem)
    populacao       BIGINT,
    area_km2        NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS dim_distribuidora (
    cod_distribuidora   VARCHAR(20) PRIMARY KEY,     -- CNPJ com 14 dígitos
    nome_distribuidora  VARCHAR(120) NOT NULL,
    cod_uf_principal    CHAR(2) REFERENCES dim_uf(cod_uf)
);

CREATE TABLE IF NOT EXISTS dim_modalidade (
    id_modalidade     INTEGER PRIMARY KEY,           -- estável: posição no catálogo (seed_dimensoes.py)
    modalidade_aneel  VARCHAR(60) NOT NULL,
    classe_consumo    VARCHAR(60) NOT NULL,
    tipo_consumidor   VARCHAR(20) NOT NULL
        CHECK (tipo_consumidor IN ('Unifamiliar','Multifamiliar','Rural','Comercial','Industrial','Outro')),
    UNIQUE (modalidade_aneel, classe_consumo)
);

CREATE TABLE IF NOT EXISTS dim_fonte_energia (
    cod_fonte    VARCHAR(20) PRIMARY KEY,
    nome_fonte   VARCHAR(80) NOT NULL,
    categoria    VARCHAR(20) NOT NULL CHECK (categoria IN ('Renovável', 'Não renovável', 'Mista'))
);

CREATE TABLE IF NOT EXISTS dim_clima_uf (
    cod_uf                  CHAR(2) NOT NULL REFERENCES dim_uf(cod_uf),
    id_tempo                INTEGER NOT NULL REFERENCES dim_tempo(id_tempo),
    irradiacao_kwh_m2_dia   NUMERIC(6,3),            -- média diária do mês; NULL = sem leitura válida
    temperatura_media_c     NUMERIC(5,2),
    precipitacao_total_mm   NUMERIC(10,2),
    cobertura_pct           NUMERIC(5,1),            -- % das horas diurnas com radiação válida
    qtd_estacoes            SMALLINT NOT NULL DEFAULT 0,
    amostra_reduzida        BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (cod_uf, id_tempo)
);

-- Estoque ACUMULADO até o fim do mês + fluxo (novas conexões no mês), solar (UFV)
CREATE TABLE IF NOT EXISTS fato_geracao_uf (
    id_tempo               INTEGER NOT NULL REFERENCES dim_tempo(id_tempo),
    cod_uf                 CHAR(2) NOT NULL REFERENCES dim_uf(cod_uf),
    id_modalidade          INTEGER NOT NULL REFERENCES dim_modalidade(id_modalidade),
    potencia_acumulada_kw  NUMERIC(16,2) NOT NULL,
    qtd_acumulada          INTEGER NOT NULL,
    potencia_nova_kw       NUMERIC(14,2) NOT NULL,
    qtd_novas_conexoes     INTEGER NOT NULL,
    potencia_media_kw      NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (id_tempo, cod_uf, id_modalidade)
);

CREATE TABLE IF NOT EXISTS fato_tarifa (
    id_tempo                INTEGER NOT NULL REFERENCES dim_tempo(id_tempo),
    cod_uf                  CHAR(2) NOT NULL REFERENCES dim_uf(cod_uf),
    tarifa_media_reais_kwh  NUMERIC(8,4) NOT NULL,   -- R$/kWh nominal (B1 residencial convencional)
    tarifa_deflacionada     NUMERIC(8,4) NOT NULL,   -- R$/kWh em reais de dez/2025 (IPCA)
    qtd_distribuidoras      SMALLINT NOT NULL,
    PRIMARY KEY (id_tempo, cod_uf)
);

CREATE TABLE IF NOT EXISTS fato_matriz_energetica (
    id_tempo                 INTEGER NOT NULL REFERENCES dim_tempo(id_tempo),
    cod_fonte                VARCHAR(20) NOT NULL REFERENCES dim_fonte_energia(cod_fonte),
    participacao_percentual  NUMERIC(5,2) NOT NULL,
    geracao_gwh              NUMERIC(14,1) NOT NULL,   -- eletricidade gerada no país no ano (id_tempo = AAAA12)
    geracao_servico_publico_gwh NUMERIC(14,1) NOT NULL,
    geracao_autoprodutoras_gwh  NUMERIC(14,1) NOT NULL,
    PRIMARY KEY (id_tempo, cod_fonte)
);

-- Trilha de teste do pipeline (Etapa 3): uma linha por tabela carregada por execução
CREATE TABLE IF NOT EXISTS etl_execucao (
    id_execucao   BIGSERIAL PRIMARY KEY,
    execucao_ref  VARCHAR(80) NOT NULL,              
    tabela        VARCHAR(60) NOT NULL,
    linhas        INTEGER NOT NULL,
    inseridas     INTEGER,
    atualizadas   INTEGER,
    carregado_em  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fato_geracao_uf_uf    ON fato_geracao_uf (cod_uf);
CREATE INDEX IF NOT EXISTS idx_fato_geracao_uf_tempo ON fato_geracao_uf (id_tempo);
CREATE INDEX IF NOT EXISTS idx_fato_tarifa_uf        ON fato_tarifa (cod_uf);
CREATE INDEX IF NOT EXISTS idx_dim_clima_uf_uf       ON dim_clima_uf (cod_uf);
