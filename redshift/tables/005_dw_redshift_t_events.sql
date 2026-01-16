-- =============================================================
-- Tabela: dw_redshift.t_events
-- Camada: DW (Redshift)
-- =============================================================

CREATE SCHEMA IF NOT EXISTS dw_redshift;

DROP TABLE IF EXISTS dw_redshift.t_events;

CREATE TABLE dw_redshift.t_events (
    sk_event bigint identity(1, 1) ENCODE az64 DISTKEY,
    event_id bigint ENCODE az64,
    vehicle_id bigint ENCODE az64,
    scheme_id bigint ENCODE az64,
    drive_id bigint ENCODE az64,
    start_time timestamp without time zone ENCODE az64,
    start_location character varying(500) ENCODE lzo COLLATE case_sensitive,
    start_latitude double precision ENCODE raw,
    start_longitude double precision ENCODE raw,
    direction double precision ENCODE raw,
    start_mileage double precision ENCODE raw,
    end_time timestamp without time zone ENCODE az64,
    end_location character varying(500) ENCODE lzo COLLATE case_sensitive,
    end_latitude double precision ENCODE raw,
    end_longitude double precision ENCODE raw,
    end_mileage double precision ENCODE raw,
    speed double precision ENCODE raw,
    max_speed double precision ENCODE raw,
    p_source_date date ENCODE az64,
    p_load_date date DEFAULT ('now'::text)::date ENCODE az64,
    FOREIGN KEY (scheme_id) REFERENCES dw_redshift.d_schemes(scheme_id)
)
DISTSTYLE AUTO;
