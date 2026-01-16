-- =============================================================
-- Materialized View: dw_redshift.mv_fleet_safety_monthly_agg
-- Objetivo: segurança mensal agregada por grupo (pontos por km) + tendência vs mês anterior
-- Também retorna o motorista com maior pontuação no mês (top_driver) para contexto.
-- Refresh: admin.refresh_all_mvs()
-- Dependências:
--   - dw_redshift.t_trips
--   - dw_redshift.d_vehicles
--   - dw_redshift.d_groups
--   - dw_redshift.d_drivers
-- =============================================================

CREATE SCHEMA IF NOT EXISTS dw_redshift;

DROP MATERIALIZED VIEW IF EXISTS dw_redshift.mv_fleet_safety_monthly_agg;

CREATE MATERIALIZED VIEW dw_redshift.mv_fleet_safety_monthly_agg
AUTO REFRESH NO
AS
WITH base AS (
    SELECT
        v.group_id,
        g.group_name,
        DATE_TRUNC('month', t.end_drive)::date AS ym,
        DATE_PART('year',  t.end_drive) AS year,
        DATE_PART('month', t.end_drive) AS month,
        t.driver_id,
        COALESCE(t.turn1, 0) + COALESCE(t.turn2, 0) + COALESCE(t.turn3, 0)
      + COALESCE(t.break1, 0) + COALESCE(t.break2, 0) + COALESCE(t.break3, 0)
      + COALESCE(t.acceleration1, 0) + COALESCE(t.acceleration2, 0) + COALESCE(t.acceleration3, 0)
      + COALESCE(t.speed_road1, 0) + COALESCE(t.speed_road2, 0) + COALESCE(t.speed_road3, 0) AS safety_points,
        COALESCE(t.mileage, 0::double precision) AS km
    FROM dw_redshift.t_trips t
    LEFT JOIN dw_redshift.d_vehicles v ON v.vehicle_id = t.vehicle_id
    LEFT JOIN dw_redshift.d_groups g   ON g.group_id = v.group_id
    WHERE t.end_drive IS NOT NULL
),
agg_fleet AS (
    SELECT
        group_id,
        group_name,
        year,
        month,
        ym,
        SUM(safety_points) AS total_points,
        SUM(km) AS total_km,
        CASE
            WHEN SUM(km) > 0 THEN SUM(safety_points)::double precision / SUM(km) * 100
        END AS safety_pct
    FROM base
    GROUP BY
        group_id,
        group_name,
        year,
        month,
        ym
),
with_prev AS (
    SELECT
        *,
        LEAD(safety_pct) OVER (
            PARTITION BY group_id
            ORDER BY year DESC, month DESC
        ) AS safety_pct_prev
    FROM agg_fleet
),
ranked_fleet AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY group_id
            ORDER BY year DESC, month DESC
        ) AS rn
    FROM with_prev
),
driver_monthly AS (
    SELECT
        v.group_id,
        DATE_TRUNC('month', t.end_drive)::date AS ym,
        t.driver_id,
        SUM(
            COALESCE(t.turn1, 0) + COALESCE(t.turn2, 0) + COALESCE(t.turn3, 0)
          + COALESCE(t.break1, 0) + COALESCE(t.break2, 0) + COALESCE(t.break3, 0)
          + COALESCE(t.acceleration1, 0) + COALESCE(t.acceleration2, 0) + COALESCE(t.acceleration3, 0)
          + COALESCE(t.speed_road1, 0) + COALESCE(t.speed_road2, 0) + COALESCE(t.speed_road3, 0)
        ) AS driver_points
    FROM dw_redshift.t_trips t
    JOIN dw_redshift.d_vehicles v ON v.vehicle_id = t.vehicle_id
    WHERE t.end_drive IS NOT NULL
      AND t.driver_id IS NOT NULL
    GROUP BY
        v.group_id,
        DATE_TRUNC('month', t.end_drive),
        t.driver_id
),
top_driver_monthly AS (
    SELECT
        group_id,
        ym,
        driver_id
    FROM (
        SELECT
            group_id,
            ym,
            driver_id,
            ROW_NUMBER() OVER (
                PARTITION BY group_id, ym
                ORDER BY driver_points DESC
            ) AS rn
        FROM driver_monthly
    ) x
    WHERE rn = 1
),
drivers_dedup AS (
    SELECT
        driver_id,
        MAX(driver_name) AS driver_name
    FROM dw_redshift.d_drivers
    GROUP BY driver_id
)
SELECT
    r.group_id,
    r.group_name,
    r.year  AS current_year,
    r.month AS current_month,
    r.total_points,
    r.total_km,
    r.safety_pct,
    r.safety_pct_prev,
    CASE
        WHEN r.safety_pct_prev IS NULL THEN 'sem histórico'
        WHEN r.safety_pct < r.safety_pct_prev THEN 'melhorou'
        WHEN r.safety_pct > r.safety_pct_prev THEN 'piorou'
        ELSE 'igual'
    END AS safety_tendencia,
    CASE
        WHEN r.safety_pct_prev IS NULL THEN 'Sem histórico anterior para comparação.'
        WHEN r.safety_pct < r.safety_pct_prev THEN
            'A segurança da frota melhorou ' || ROUND(ABS(r.safety_pct - r.safety_pct_prev), 2)::text || '% em relação ao mês anterior.'
        WHEN r.safety_pct > r.safety_pct_prev THEN
            'A segurança da frota piorou ' || ROUND(ABS(r.safety_pct - r.safety_pct_prev), 2)::text || '% em relação ao mês anterior.'
        ELSE 'A segurança da frota permaneceu estável em relação ao mês anterior.'
    END AS safety_message,
    td.driver_id AS top_driver_id,
    d.driver_name AS top_driver_name
FROM ranked_fleet r
LEFT JOIN top_driver_monthly td
    ON td.group_id = r.group_id
   AND td.ym = r.ym
LEFT JOIN drivers_dedup d
    ON d.driver_id = td.driver_id
WHERE r.rn = 1;
