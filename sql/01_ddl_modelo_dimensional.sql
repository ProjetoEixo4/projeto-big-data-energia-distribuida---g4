-- =============================================================================
-- Modelo Dimensional — Constelação de Fatos (granularidade UF)
-- Observatório de Energia e Transição Digital (OET)
-- =============================================================================

CREATE TABLE dim_tempo (
    id_tempo    INTEGER PRIMARY KEY,
    ano         SMALLINT NOT NULL,
    mes         SMALLINT NOT NULL CHECK (mes BETWEEN 1 AND 12),
    trimestre   SMALLINT NOT NULL CHECK (trimestre BETWEEN 1 AND 4)
);

CREATE TABLE dim_uf (
    cod_uf          CHAR(2) PRIMARY KEY,
    nome_uf         VARCHAR(40) NOT NULL,
    regiao          VARCHAR(20) NOT NULL,
    pib_per_capita  NUMERIC(12,2),
    pib_projetado   BOOLEAN NOT NULL DEFAULT FALSE,
    populacao       BIGINT,
    area_km2        NUMERIC(12,2)
);

CREATE TABLE dim_distribuidora (
    cod_distribuidora   VARCHAR(20) PRIMARY KEY,
    nome_distribuidora  VARCHAR(120) NOT NULL,
    cod_uf_principal    CHAR(2) REFERENCES dim_uf(cod_uf)
);

CREATE TABLE dim_modalidade (
    id_modalidade     SERIAL PRIMARY KEY,
    modalidade_aneel  VARCHAR(60) NOT NULL,
    classe_consumo    VARCHAR(60),
    tipo_consumidor   VARCHAR(20) NOT NULL
        CHECK (tipo_consumidor IN ('Unifamiliar', 'Multifamiliar', 'Rural', 'Outro'))
);

CREATE TABLE dim_fonte_energia (
    cod_fonte    VARCHAR(20) PRIMARY KEY,
    nome_fonte   VARCHAR(40) NOT NULL,
    categoria    VARCHAR(20) NOT NULL CHECK (categoria IN ('Renovável', 'Não renovável'))
);

CREATE TABLE dim_clima_uf (
    cod_uf                  CHAR(2) NOT NULL REFERENCES dim_uf(cod_uf),
    id_tempo                INTEGER NOT NULL REFERENCES dim_tempo(id_tempo),
    irradiacao_media_kjm2   NUMERIC(10,2),
    temperatura_media_c     NUMERIC(5,2),
    precipitacao_total_mm   NUMERIC(10,2),
    qtd_estacoes            SMALLINT NOT NULL DEFAULT 0,
    amostra_reduzida        BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (cod_uf, id_tempo)
);

CREATE TABLE fato_geracao_uf (
    id_tempo          INTEGER NOT NULL REFERENCES dim_tempo(id_tempo),
    cod_uf            CHAR(2) NOT NULL REFERENCES dim_uf(cod_uf),
    id_modalidade     INTEGER NOT NULL REFERENCES dim_modalidade(id_modalidade),
    potencia_total_kw NUMERIC(14,2) NOT NULL,
    qtd_conexoes      INTEGER NOT NULL,
    potencia_media_kw NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (id_tempo, cod_uf, id_modalidade)
);

CREATE TABLE fato_tarifa (
    id_tempo                INTEGER NOT NULL REFERENCES dim_tempo(id_tempo),
    cod_uf                  CHAR(2) NOT NULL REFERENCES dim_uf(cod_uf),
    tarifa_media_reais_kwh  NUMERIC(8,4) NOT NULL,
    tarifa_deflacionada     NUMERIC(8,4) NOT NULL,
    PRIMARY KEY (id_tempo, cod_uf)
);

CREATE TABLE fato_matriz_energetica (
    id_tempo               INTEGER NOT NULL REFERENCES dim_tempo(id_tempo),
    cod_fonte              VARCHAR(20) NOT NULL REFERENCES dim_fonte_energia(cod_fonte),
    participacao_percentual NUMERIC(5,2) NOT NULL,
    geracao_gwh            NUMERIC(12,2) NOT NULL,
    PRIMARY KEY (id_tempo, cod_fonte)
);

CREATE INDEX idx_fato_geracao_uf_uf   ON fato_geracao_uf (cod_uf);
CREATE INDEX idx_fato_geracao_uf_tempo ON fato_geracao_uf (id_tempo);
CREATE INDEX idx_fato_tarifa_uf       ON fato_tarifa (cod_uf);
CREATE INDEX idx_dim_clima_uf_uf      ON dim_clima_uf (cod_uf);