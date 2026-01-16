-- =============================================================
-- Tabela: dw_redshift.d_event_dictionary
-- Camada: DW (Redshift) - Dicionário de eventos / boas práticas
-- =============================================================

CREATE SCHEMA IF NOT EXISTS dw_redshift;

DROP TABLE IF EXISTS dw_redshift.d_event_dictionary;

CREATE TABLE dw_redshift.d_event_dictionary (
    parametro character varying(100) ENCODE lzo COLLATE case_sensitive,
    categoria character varying(50) ENCODE lzo COLLATE case_sensitive,
    impacto_resumido character varying(500) ENCODE lzo COLLATE case_sensitive,
    acao_recomendada_final character varying(500) ENCODE lzo COLLATE case_sensitive,
    peso_impacto double precision ENCODE raw
)
DISTSTYLE AUTO;
