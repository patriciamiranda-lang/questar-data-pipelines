-- =============================================================
-- Materialized View: dw_redshift.mv_group_main_driver_monthly
-- Objetivo: identificar o motorista “principal” por grupo e mês (maior km)
-- Observação: mantém rn (rank) por (group_id, ym).
-- Refresh: admin.refresh_all_mvs()
-- Dependências:
--   - dw_redshift.t_trips
--   - dw_redshift.d_drivers
--   - dw_redshift.d_vehicles
--   - dw_redshift.d_groups
-- =============================================================

CREATE SCHEMA IF NOT EXISTS dw_redshift;

DROP MATERIALIZED VIEW IF EXISTS dw_redshift.mv_group_main_driver_monthly;

CREATE MATERIALIZED VIEW dw_redshift.mv_group_main_driver_monthly
SORTKEY (group_id, ym)
AS
WITH base AS (
    SELECT
        g.group_id,
        DATE_TRUNC('month', t.end_drive)::date AS ym,
        d.driver_id,
        d.driver_name,
        COALESCE(t.mileage, 0) AS km
    FROM dw_redshift.t_trips t
    JOIN dw_redshift.d_drivers d  ON d.driver_id = t.driver_id
    JOIN dw_redshift.d_vehicles v ON v.vehicle_id = t.vehicle_id
    JOIN dw_redshift.d_groups g   ON g.group_id = v.group_id
    WHERE t.end_drive IS NOT NULL
)
SELECT
    group_id,
    ym,
    driver_id,
    driver_name,
    SUM(km) AS total_km,
    ROW_NUMBER() OVER (
        PARTITION BY group_id, ym
        ORDER BY SUM(km) DESC
    ) AS rn
FROM base
GROUP BY
    group_id,
    ym,
    driver_id,
    driver_name;
