-- =============================================================
-- Tabela: dw_redshift.parametro_base
-- Camada: DW (Redshift) - Base de parâmetros
-- =============================================================

CREATE SCHEMA IF NOT EXISTS dw_redshift;

DROP TABLE IF EXISTS dw_redshift.parametro_base;

CREATE TABLE dw_redshift.parametro_base (
    parametro character varying(100) ENCODE lzo COLLATE case_sensitive,
    categoria character varying(50) ENCODE lzo COLLATE case_sensitive,
    peso_impacto integer ENCODE az64,
    acao_recomendada_final character varying(500) ENCODE lzo COLLATE case_sensitive
)
DISTSTYLE AUTO;
