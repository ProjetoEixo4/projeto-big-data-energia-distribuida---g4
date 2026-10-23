
-- Migração Etapa 2 -> Etapa 3 
BEGIN;

DROP TABLE IF EXISTS fato_geracao_uf        CASCADE;  -- passou de "cadastros do mês" para estoque acumulado + fluxo
DROP TABLE IF EXISTS fato_tarifa            CASCADE;  -- ganhou qtd_distribuidoras; deflacionada deixa de ser cópia
DROP TABLE IF EXISTS fato_matriz_energetica CASCADE;  -- passa de energia primária (8 fontes) a geração elétrica (4 fontes)
DROP TABLE IF EXISTS dim_clima_uf           CASCADE;  -- kJ/m² médio -> kWh/m²/dia + cobertura_pct
DROP TABLE IF EXISTS dim_fonte_energia      CASCADE;  -- 8 fontes primárias -> 4 fontes da geração elétrica
DROP TABLE IF EXISTS dim_modalidade         CASCADE;  -- SERIAL -> id estável do catálogo fixo

ALTER TABLE IF EXISTS dim_uf ADD COLUMN IF NOT EXISTS ano_ref_pib SMALLINT;

COMMIT;
