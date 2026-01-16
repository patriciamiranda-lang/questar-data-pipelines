-- =============================================================
-- Tabela: dw_redshift.d_schemes
-- Camada: DW (Redshift)
-- =============================================================

CREATE SCHEMA IF NOT EXISTS dw_redshift;

DROP TABLE IF EXISTS dw_redshift.d_schemes;

CREATE TABLE dw_redshift.d_schemes (
    scheme_id bigint NOT NULL ENCODE raw DISTKEY,
    scheme_description character varying(600) ENCODE lzo COLLATE case_sensitive,
    description character varying(600) ENCODE lzo COLLATE case_sensitive,
    parent_scheme character varying(300) ENCODE lzo COLLATE case_sensitive,
    scheme_type character varying(200) ENCODE bytedict COLLATE case_sensitive,
    p_load_date character varying(20) ENCODE lzo COLLATE case_sensitive,
    p_source_date date ENCODE az64,
    PRIMARY KEY (scheme_id)
)
DISTSTYLE AUTO
SORTKEY (scheme_id);
