-- =========================================================
-- OET | Modelo dimensional — Tabelas de Fato (PostgreSQL)
-- Constelação de fatos: 3 fatos compartilhando DIM_TEMPO
-- Etapa 2 — Replicação e Integração de Dados
-- =========================================================

-- -------------------------------------------------------
-- FATO_GERACAO_DISTRIBUIDA
-- Grão: 1 empreendimento de geração distribuída conectado (ANEEL).
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS oet.fato_geracao_distribuida (
    cod_geracao         VARCHAR(30) PRIMARY KEY,   -- id do empreendimento na ANEEL
    id_tempo            INTEGER NOT NULL REFERENCES oet.dim_tempo(id_tempo),
    cod_municipio_ibge  VARCHAR(7) NOT NULL REFERENCES oet.dim_municipio(cod_municipio_ibge),
    cod_distribuidora   VARCHAR(20) NOT NULL REFERENCES oet.dim_distribuidora(cod_distribuidora),
    id_modalidade       INTEGER NOT NULL REFERENCES oet.dim_modalidade(id_modalidade),
    potencia_instalada_kw   NUMERIC(12,3) NOT NULL,
    qtd_modulos             INTEGER,
    data_conexao            DATE,
    carga_atualizada_em     TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_fgd_tempo       ON oet.fato_geracao_distribuida (id_tempo);
CREATE INDEX IF NOT EXISTS ix_fgd_municipio   ON oet.fato_geracao_distribuida (cod_municipio_ibge);
CREATE INDEX IF NOT EXISTS ix_fgd_modalidade  ON oet.fato_geracao_distribuida (id_modalidade);

-- -------------------------------------------------------
-- FATO_TARIFA
-- Grão: 1 distribuidora x mês/ano (ANEEL — Tarifas de Energia).
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS oet.fato_tarifa (
    id_tempo            INTEGER NOT NULL REFERENCES oet.dim_tempo(id_tempo),
    cod_distribuidora   VARCHAR(20) NOT NULL REFERENCES oet.dim_distribuidora(cod_distribuidora),
    tarifa_reais_kwh        NUMERIC(10,6) NOT NULL,   -- valor corrente
    tarifa_deflacionada     NUMERIC(10,6),            -- ajustada pelo IPCA (ano-base definido no ETL)
    carga_atualizada_em     TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (id_tempo, cod_distribuidora)
);

-- -------------------------------------------------------
-- FATO_MATRIZ_ENERGETICA
-- Grão: 1 fonte de energia x ano (EPE — Balanço Energético Nacional).
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS oet.fato_matriz_energetica (
    id_tempo            INTEGER NOT NULL REFERENCES oet.dim_tempo(id_tempo),
    cod_fonte           VARCHAR(20) NOT NULL REFERENCES oet.dim_fonte_energia(cod_fonte),
    participacao_percentual    NUMERIC(6,3) NOT NULL,
    geracao_gwh                NUMERIC(14,3),
    carga_atualizada_em        TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (id_tempo, cod_fonte)
);

-- -------------------------------------------------------
-- Tabela de apoio: registro de referência (SEM script de extração associado)
-- ABSOLAR e Greener foram avaliadas e excluídas do pipeline automatizado
-- (ver README.md — "Nota sobre ABSOLAR e Greener"): ABSOLAR exige cadastro
-- prévio não respondido, e a Greener não publica dados em formato aberto.
-- Esta tabela guarda apenas valores citados pontualmente a partir de
-- literatura secundária (imprensa especializada, publicações do BNDES),
-- inseridos manualmente quando relevante para a discussão do projeto.
-- Grão: 1 ano x faixa de potência — custo médio por Wp instalado.
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS oet.apoio_custo_instalacao (
    ano                 SMALLINT NOT NULL,
    faixa_potencia      VARCHAR(30) NOT NULL,   -- 'Residencial', 'Comercial', ...
    preco_medio_wp      NUMERIC(8,4) NOT NULL,
    fonte               VARCHAR(20) NOT NULL,   -- 'ABSOLAR' | 'Greener'
    PRIMARY KEY (ano, faixa_potencia, fonte)
);
