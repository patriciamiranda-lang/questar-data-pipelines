-- =============================================================
-- Tabela: dw_redshift.d_drivers
-- Camada: DW (Redshift)
-- =============================================================

CREATE SCHEMA IF NOT EXISTS dw_redshift;

DROP TABLE IF EXISTS dw_redshift.d_drivers;

CREATE TABLE dw_redshift.d_drivers (
    driver_id bigint NOT NULL ENCODE raw DISTKEY,
    driver_name character varying(255) ENCODE lzo COLLATE case_sensitive,
    group_id bigint ENCODE az64,
    driver_code character varying(255) ENCODE lzo COLLATE case_sensitive,
    worker_id bigint ENCODE az64,
    p_source_date character varying(50) ENCODE lzo COLLATE case_sensitive,
    PRIMARY KEY (driver_id)
)
DISTSTYLE AUTO
SORTKEY (driver_id);
