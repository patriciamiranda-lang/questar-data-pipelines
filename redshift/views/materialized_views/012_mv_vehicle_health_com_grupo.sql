-- =============================================================
-- Materialized View: dw_redshift.mv_vehicle_health_com_grupo
-- Objetivo: saúde do veículo (pontos ponderados por tipo de evento) + grupo
-- Pesos:
--   engine=5, braking=4, gearbox=4, clutch=3
-- Classificação:
--   <=300  => Crítico
--   <=1000 => Atenção
--   >1000  => OK
-- Refresh: admin.refresh_all_mvs()
-- Dependências:
--   - dw_redshift.t_events
--   - dw_redshift.d_schemes
--   - dw_redshift.d_vehicles
--   - dw_redshift.d_groups
-- =============================================================

CREATE SCHEMA IF NOT EXISTS dw_redshift;

DROP MATERIALIZED VIEW IF EXISTS dw_redshift.mv_vehicle_health_com_grupo;

CREATE MATERIALIZED VIEW dw_redshift.mv_vehicle_health_com_grupo AS
WITH events_filtered AS (
    SELECT
        e.vehicle_id,
        e.start_time::date AS event_day,
        LOWER(s.parent_scheme) AS parent_raw
    FROM dw_redshift.t_events e
    LEFT JOIN dw_redshift.d_schemes s ON e.scheme_id = s.scheme_id
),
classified_and_filtered AS (
    SELECT
        vehicle_id,
        event_day,
        CASE
            WHEN parent_raw LIKE '%engine%' THEN 'engine'
            WHEN parent_raw LIKE '%brak%' OR parent_raw LIKE '%abs%' THEN 'braking'
            WHEN parent_raw LIKE '%gear%' OR parent_raw LIKE '%transmission%' THEN 'gearbox'
            WHEN parent_raw LIKE '%clutch%' THEN 'clutch'
            WHEN parent_raw LIKE '%electric%' THEN 'electrical'
            WHEN parent_raw LIKE '%suspension%' THEN 'suspension'
            ELSE 'other'
        END AS event_type
    FROM events_filtered
    WHERE parent_raw IS NOT NULL
),
agg_status AS (
    SELECT
        vehicle_id,
        SUM(CASE WHEN event_type = 'engine'  THEN 1 ELSE 0 END) AS engine_events,
        SUM(CASE WHEN event_type = 'braking' THEN 1 ELSE 0 END) AS braking_events,
        SUM(CASE WHEN event_type = 'gearbox' THEN 1 ELSE 0 END) AS gearbox_events,
        SUM(CASE WHEN event_type = 'clutch'  THEN 1 ELSE 0 END) AS clutch_events,
        MAX(event_day) AS last_seen,
        (
            SUM(CASE WHEN event_type = 'engine'  THEN 1 ELSE 0 END) * 5
          + SUM(CASE WHEN event_type = 'braking' THEN 1 ELSE 0 END) * 4
          + SUM(CASE WHEN event_type = 'gearbox' THEN 1 ELSE 0 END) * 4
          + SUM(CASE WHEN event_type = 'clutch'  THEN 1 ELSE 0 END) * 3
        ) AS health_points
    FROM classified_and_filtered
    WHERE event_type IN ('engine', 'braking', 'gearbox', 'clutch')
    GROUP BY vehicle_id
)
SELECT
    a.vehicle_id,
    v.group_id,
    g.group_name,
    a.engine_events,
    a.braking_events,
    a.gearbox_events,
    a.clutch_events,
    a.health_points,
    CASE
        WHEN a.health_points <= 300  THEN 'Crítico'
        WHEN a.health_points <= 1000 THEN 'Atenção'
        ELSE 'OK'
    END AS health_status,
    a.last_seen
FROM agg_status a
LEFT JOIN dw_redshift.d_vehicles v ON v.vehicle_id = a.vehicle_id
LEFT JOIN dw_redshift.d_groups g   ON g.group_id = v.group_id;
