-- =============================================================
-- Tabela: dw_redshift.d_vehicles
-- Camada: DW (Redshift)
-- =============================================================

CREATE SCHEMA IF NOT EXISTS dw_redshift;

DROP TABLE IF EXISTS dw_redshift.d_vehicles;

CREATE TABLE dw_redshift.d_vehicles (
    vehicle_id bigint ENCODE az64,
    license_number character varying(16383) ENCODE lzo COLLATE case_sensitive,
    group_id bigint ENCODE az64,
    manufacturer character varying(16383) ENCODE bytedict COLLATE case_sensitive,
    model character varying(16383) ENCODE lzo COLLATE case_sensitive,
    unit_serial_number bigint ENCODE az64,
    vin character varying(16383) ENCODE lzo COLLATE case_sensitive,
    vehicle_engine_type character varying(16383) ENCODE bytedict COLLATE case_sensitive,
    body_configuration character varying(16383) ENCODE lzo COLLATE case_sensitive,
    p_source_date character varying(16383) ENCODE bytedict COLLATE case_sensitive
)
DISTSTYLE AUTO;
