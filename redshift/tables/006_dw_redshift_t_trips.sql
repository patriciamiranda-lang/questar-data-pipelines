-- =============================================================
-- Tabela: dw_redshift.t_trips
-- Camada: DW (Redshift)
-- =============================================================

CREATE SCHEMA IF NOT EXISTS dw_redshift;

DROP TABLE IF EXISTS dw_redshift.t_trips;

CREATE TABLE dw_redshift.t_trips (
    drive_id bigint ENCODE raw,
    vehicle_id bigint ENCODE raw,
    driver_id bigint ENCODE raw,
    start_drive timestamp without time zone ENCODE raw,
    start_location character varying(16383) ENCODE raw COLLATE case_sensitive,
    start_latitude double precision ENCODE raw,
    start_longitude double precision ENCODE raw,
    end_drive timestamp without time zone ENCODE raw,
    end_location character varying(16383) ENCODE raw COLLATE case_sensitive,
    end_latitude double precision ENCODE raw,
    end_longitude double precision ENCODE raw,
    drive_duration double precision ENCODE raw,
    idle_duration double precision ENCODE raw,
    mileage double precision ENCODE raw,
    avg_speed double precision ENCODE raw,
    turn1 integer ENCODE raw,
    turn2 integer ENCODE raw,
    turn3 integer ENCODE raw,
    break1 integer ENCODE raw,
    break2 integer ENCODE raw,
    break3 integer ENCODE raw,
    acceleration1 integer ENCODE raw,
    acceleration2 integer ENCODE raw,
    acceleration3 integer ENCODE raw,
    speed_road1 integer ENCODE raw,
    speed_road2 integer ENCODE raw,
    speed_road3 integer ENCODE raw,
    energy_consumption integer ENCODE raw,
    breaking_system integer ENCODE raw,
    gearbox integer ENCODE raw,
    engine integer ENCODE raw,
    clutch_system integer ENCODE raw,
    start_energy_level double precision ENCODE raw,
    end_energy_level double precision ENCODE raw,
    energy_used double precision ENCODE raw,
    start_soc double precision ENCODE raw,
    end_soc double precision ENCODE raw,
    p_source_date character varying(16383) ENCODE raw COLLATE case_sensitive
)
DISTSTYLE EVEN;
