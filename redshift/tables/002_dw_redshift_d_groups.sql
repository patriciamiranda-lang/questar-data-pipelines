-- =============================================================
-- Tabela: dw_redshift.d_groups
-- Camada: DW (Redshift)
-- =============================================================

CREATE SCHEMA IF NOT EXISTS dw_redshift;

DROP TABLE IF EXISTS dw_redshift.d_groups;

CREATE TABLE dw_redshift.d_groups (
    group_id bigint NOT NULL ENCODE raw DISTKEY,
    group_name character varying(200) ENCODE bytedict COLLATE case_sensitive,
    parent_group_id bigint ENCODE az64,
    parent_group_name character varying(200) ENCODE bytedict COLLATE case_sensitive,
    client_id bigint ENCODE az64,
    p_source_date character varying(20) ENCODE bytedict COLLATE case_sensitive,
    external_id character varying(50) ENCODE lzo COLLATE case_sensitive,
    PRIMARY KEY (group_id)
)
DISTSTYLE AUTO
SORTKEY (group_id);
