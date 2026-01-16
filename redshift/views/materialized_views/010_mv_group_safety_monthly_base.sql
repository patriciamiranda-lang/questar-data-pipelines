-- =============================================================
-- Materialized View: dw_redshift.mv_group_safety_monthly_base
-- Objetivo: base mensal de segurança por grupo (pontos por km * 100)
-- Refresh: admin.refresh_all_mvs()
-- Dependências:
--   - dw_redshift.t_trips
--   - dw_redshift.d_vehicles
--   - dw_redshift.d_groups
-- =============================================================

CREATE SCHEMA IF NOT EXISTS dw_redshift;

DROP MATERIALIZED VIEW IF EXISTS dw_redshift.mv_group_safety_monthly_base;

CREATE MATERIALIZED VIEW dw_redshift.mv_group_safety_monthly_base
SORTKEY (group_id, ym)
AS
SELECT
    v.group_id,
    g.group_name,
    DATE_TRUNC('month', t.end_drive)::date AS ym,
    DATE_PART(year,  t.end_drive)::int AS year,
    DATE_PART(month, t.end_drive)::int AS month,
    SUM(
        COALESCE(t.turn1, 0) + COALESCE(t.turn2, 0) + COALESCE(t.turn3, 0)
      + COALESCE(t.break1, 0) + COALESCE(t.break2, 0) + COALESCE(t.break3, 0)
      + COALESCE(t.acceleration1, 0) + COALESCE(t.acceleration2, 0) + COALESCE(t.acceleration3, 0)
      + COALESCE(t.speed_road1, 0) + COALESCE(t.speed_road2, 0) + COALESCE(t.speed_road3, 0)
    ) AS total_points,
    SUM(COALESCE(t.mileage, 0)) AS total_km,
    CASE
        WHEN SUM(COALESCE(t.mileage, 0)) > 0 THEN
            SUM(
                COALESCE(t.turn1, 0) + COALESCE(t.turn2, 0) + COALESCE(t.turn3, 0)
              + COALESCE(t.break1, 0) + COALESCE(t.break2, 0) + COALESCE(t.break3, 0)
              + COALESCE(t.acceleration1, 0) + COALESCE(t.acceleration2, 0) + COALESCE(t.acceleration3, 0)
              + COALESCE(t.speed_road1, 0) + COALESCE(t.speed_road2, 0) + COALESCE(t.speed_road3, 0)
            ) / SUM(COALESCE(t.mileage, 0)) * 100
        ELSE NULL
    END AS safety_pct
FROM dw_redshift.t_trips t
LEFT JOIN dw_redshift.d_vehicles v ON v.vehicle_id = t.vehicle_id
LEFT JOIN dw_redshift.d_groups g   ON g.group_id = v.group_id
WHERE t.end_drive IS NOT NULL
GROUP BY
    v.group_id,
    g.group_name,
    ym,
    year,
    month;
