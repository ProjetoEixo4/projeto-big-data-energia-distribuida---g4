-- =========================================================
-- OET | Modelo dimensional — Tabelas de Dimensão (PostgreSQL)
-- Etapa 2 — Replicação e Integração de Dados
-- =========================================================

CREATE SCHEMA IF NOT EXISTS oet;

-- -------------------------------------------------------
-- DIM_TEMPO
-- Grão: 1 mês/ano. Compartilhada por todos os fatos.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS oet.dim_tempo (
    id_tempo        INTEGER PRIMARY KEY,      -- formato AAAAMM, ex.: 202403
    ano             SMALLINT NOT NULL,
    mes             SMALLINT NOT NULL,
    trimestre       SMALLINT NOT NULL,
    UNIQUE (ano, mes)
);

-- Linha "desconhecido" para evitar NULLs em FK (padrão de Data Warehouse)
INSERT INTO oet.dim_tempo (id_tempo, ano, mes, trimestre)
VALUES (-1, -1, -1, -1)
ON CONFLICT (id_tempo) DO NOTHING;

-- -------------------------------------------------------
-- DIM_MUNICIPIO
-- Grão: 1 município (código IBGE). Enriquecida com IBGE (PIB/população).
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS oet.dim_municipio (
    cod_municipio_ibge  VARCHAR(7) PRIMARY KEY,
    nome_municipio      VARCHAR(120) NOT NULL,
    uf                  CHAR(2) NOT NULL,
    regiao              VARCHAR(20),
    pib_per_capita      NUMERIC(14,2),
    populacao           INTEGER,
    ano_referencia_pib  SMALLINT
);

INSERT INTO oet.dim_municipio (cod_municipio_ibge, nome_municipio, uf, regiao)
VALUES ('0000000', 'Não identificado', 'XX', 'Não identificado')
ON CONFLICT (cod_municipio_ibge) DO NOTHING;

-- -------------------------------------------------------
-- DIM_DISTRIBUIDORA
-- Grão: 1 distribuidora de energia (concessionária).
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS oet.dim_distribuidora (
    cod_distribuidora   VARCHAR(20) PRIMARY KEY,
    nome_distribuidora  VARCHAR(120) NOT NULL,
    uf                  CHAR(2)
);

INSERT INTO oet.dim_distribuidora (cod_distribuidora, nome_distribuidora, uf)
VALUES ('DESCONHECIDA', 'Não identificada', 'XX')
ON CONFLICT (cod_distribuidora) DO NOTHING;

-- -------------------------------------------------------
-- DIM_MODALIDADE
-- Grão: 1 combinação (classe de consumo ANEEL x tipo de consumidor derivado)
-- Mapeia a modalidade bruta da ANEEL para unifamiliar / multifamiliar / rural.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS oet.dim_modalidade (
    id_modalidade     SERIAL PRIMARY KEY,
    modalidade_aneel  VARCHAR(60) NOT NULL,   -- ex.: 'Geração na própria UC', 'EMUC'
    classe_consumo    VARCHAR(40) NOT NULL,   -- ex.: 'Residencial', 'Rural', 'Comercial'
    tipo_consumidor   VARCHAR(20) NOT NULL,   -- 'Unifamiliar' | 'Multifamiliar' | 'Rural' | 'Outro'
    UNIQUE (modalidade_aneel, classe_consumo)
);

-- -------------------------------------------------------
-- DIM_FONTE_ENERGIA
-- Grão: 1 fonte de geração de energia (BEN/EPE).
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS oet.dim_fonte_energia (
    cod_fonte     VARCHAR(20) PRIMARY KEY,
    nome_fonte    VARCHAR(60) NOT NULL,     -- Solar, Hidráulica, Eólica, Térmica, Biomassa...
    categoria     VARCHAR(20) NOT NULL      -- Renovável | Não renovável
);

-- -------------------------------------------------------
-- DIM_CLIMA
-- Grão: 1 estação meteorológica x mês/ano. Ligada a DIM_MUNICIPIO
-- via geoprocessamento (estação -> município mais próximo).
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS oet.dim_clima (
    cod_estacao         VARCHAR(20) NOT NULL,
    id_tempo            INTEGER NOT NULL REFERENCES oet.dim_tempo(id_tempo),
    cod_municipio_ibge  VARCHAR(7) NOT NULL REFERENCES oet.dim_municipio(cod_municipio_ibge),
    irradiacao_media    NUMERIC(8,3),
    temperatura_media   NUMERIC(6,2),
    precipitacao_mm     NUMERIC(8,2),
    PRIMARY KEY (cod_estacao, id_tempo)
);
