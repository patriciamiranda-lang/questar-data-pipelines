-- =============================================================
-- Materialized View: dw_redshift.mv_ranking_motoristas
-- Objetivo: ranking/score do motorista com base em volume de eventos (Safety/Economy/Outros)
-- Refresh: admin.refresh_all_mvs()
-- Dependências:
--   - dw_redshift.t_events
--   - dw_redshift.t_trips
--   - dw_redshift.d_drivers
--   - dw_redshift.d_groups
--   - dw_redshift.d_schemes
-- =============================================================

CREATE SCHEMA IF NOT EXISTS dw_redshift;

DROP MATERIALIZED VIEW IF EXISTS dw_redshift.mv_ranking_motoristas;

CREATE MATERIALIZED VIEW dw_redshift.mv_ranking_motoristas
DISTKEY (driver_id)
SORTKEY (driver_id)
AS
WITH event_base AS (
    SELECT
        t.driver_id,
        d.driver_name,
        g.group_name,
        s.scheme_type
    FROM dw_redshift.t_events e
    JOIN dw_redshift.t_trips t     ON t.drive_id = e.drive_id
    JOIN dw_redshift.d_drivers d   ON d.driver_id = t.driver_id
    LEFT JOIN dw_redshift.d_groups g  ON g.group_id = d.group_id
    LEFT JOIN dw_redshift.d_schemes s ON s.scheme_id = e.scheme_id
)
SELECT
    driver_id,
    MAX(driver_name) AS driver_name,
    MAX(group_name)  AS group_name,
    COUNT(*) AS total_events,
    COUNT(CASE WHEN scheme_type = 'Safety'  THEN 1 END) AS safety_events,
    COUNT(CASE WHEN scheme_type = 'Economy' THEN 1 END) AS economy_events,
    (
        100
        - (COUNT(CASE WHEN scheme_type = 'Safety'  THEN 1 END) * 1.0)
        - (COUNT(CASE WHEN scheme_type = 'Economy' THEN 1 END) * 0.5)
        - (
            (
                COUNT(*)
                - COUNT(CASE WHEN scheme_type = 'Safety'  THEN 1 END)
                - COUNT(CASE WHEN scheme_type = 'Economy' THEN 1 END)
            ) * 0.1
        )
    ) AS driver_score
FROM event_base
GROUP BY driver_id;
