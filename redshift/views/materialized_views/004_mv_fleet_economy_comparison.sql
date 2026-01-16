-- =============================================================
-- Materialized View: dw_redshift.mv_fleet_economy_comparison
-- Objetivo: comparar economia média atual vs mês anterior por grupo e tipo de motor
-- Retorna também o motorista com maior km no mês (top_driver) para contexto.
-- Refresh: admin.refresh_all_mvs()
-- Dependências:
--   - dw_redshift.t_trips
--   - dw_redshift.d_vehicles
--   - dw_redshift.d_groups
--   - dw_redshift.d_drivers
-- =============================================================

CREATE SCHEMA IF NOT EXISTS dw_redshift;

DROP MATERIALIZED VIEW IF EXISTS dw_redshift.mv_fleet_economy_comparison;

CREATE MATERIALIZED VIEW dw_redshift.mv_fleet_economy_comparison
AUTO REFRESH NO
AS
WITH base AS (
    SELECT
        v.group_id,
        g.group_name,
        v.vehicle_engine_type,
        DATE_PART('year', t.end_drive)  AS year,
        DATE_PART('month', t.end_drive) AS month,
        DATE_TRUNC('month', t.end_drive)::date AS ym,
        t.driver_id,
        COALESCE(t.mileage, 0::double precision) AS km,
        COALESCE(t.energy_used, 0::double precision) AS energy
    FROM dw_redshift.t_trips t
    JOIN dw_redshift.d_vehicles v ON v.vehicle_id = t.vehicle_id
    JOIN dw_redshift.d_groups g   ON g.group_id = v.group_id
    WHERE t.end_drive IS NOT NULL
),
fleet_agg AS (
    SELECT
        group_id,
        group_name,
        vehicle_engine_type,
        year,
        month,
        ym,
        SUM(km) AS total_km,
        SUM(energy) AS total_energy,
        CASE
            WHEN vehicle_engine_type = 'Elétrico' AND SUM(km) > 0 THEN SUM(energy) / SUM(km)
            WHEN SUM(energy) > 0 THEN SUM(km) / SUM(energy)
            ELSE NULL
        END AS avg_economy_current
    FROM base
    GROUP BY
        group_id,
        group_name,
        vehicle_engine_type,
        year,
        month,
        ym
),
with_prev AS (
    SELECT
        *,
        LEAD(avg_economy_current) OVER (
            PARTITION BY group_id, vehicle_engine_type
            ORDER BY year DESC, month DESC
        ) AS avg_economy_previous
    FROM fleet_agg
),
ranked_fleet AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY group_id, vehicle_engine_type
            ORDER BY year DESC, month DESC
        ) AS rn
    FROM with_prev
),
-- km por motorista no mês
driver_monthly AS (
    SELECT
        v.group_id,
        v.vehicle_engine_type,
        DATE_TRUNC('month', t.end_drive)::date AS ym,
        t.driver_id,
        SUM(COALESCE(t.mileage, 0)) AS driver_km
    FROM dw_redshift.t_trips t
    JOIN dw_redshift.d_vehicles v ON v.vehicle_id = t.vehicle_id
    WHERE t.end_drive IS NOT NULL
      AND t.driver_id IS NOT NULL
    GROUP BY
        v.group_id,
        v.vehicle_engine_type,
        DATE_TRUNC('month', t.end_drive),
        t.driver_id
),
-- top motorista por grupo + motor + mês
top_driver_monthly AS (
    SELECT
        group_id,
        vehicle_engine_type,
        ym,
        driver_id
    FROM (
        SELECT
            group_id,
            vehicle_engine_type,
            ym,
            driver_id,
            ROW_NUMBER() OVER (
                PARTITION BY group_id, vehicle_engine_type, ym
                ORDER BY driver_km DESC
            ) AS rn
        FROM driver_monthly
    ) x
    WHERE rn = 1
),
-- deduplicação crítica para d_drivers
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
    r.vehicle_engine_type,
    r.year,
    r.month,
    r.total_km,
    r.total_energy,
    r.avg_economy_current,
    r.avg_economy_previous,
    CASE
        WHEN r.avg_economy_previous IS NULL OR r.avg_economy_previous = 0 THEN NULL
        ELSE ROUND(
            (r.avg_economy_current - r.avg_economy_previous) / r.avg_economy_previous * 100,
            2
        )
    END AS avg_economy_change_pct,
    CASE
        WHEN r.avg_economy_previous IS NULL OR r.avg_economy_previous = 0 THEN 'Sem histórico anterior para comparação.'
        WHEN r.avg_economy_current > r.avg_economy_previous THEN
            'Seu percentual de economia melhorou ' ||
            ROUND(
                (r.avg_economy_current - r.avg_economy_previous) / r.avg_economy_previous * 100,
                2
            ) || '% em relação ao mês anterior.'
        WHEN r.avg_economy_current < r.avg_economy_previous THEN
            'Seu percentual de economia piorou ' ||
            ROUND(
                ABS((r.avg_economy_current - r.avg_economy_previous) / r.avg_economy_previous) * 100,
                2
            ) || '% em relação ao mês anterior.'
        ELSE 'Seu percentual de economia permaneceu estável em relação ao mês anterior.'
    END AS mensagem_economia,
    td.driver_id AS top_driver_id,
    dd.driver_name AS top_driver_name
FROM ranked_fleet r
LEFT JOIN top_driver_monthly td
    ON td.group_id = r.group_id
   AND td.vehicle_engine_type = r.vehicle_engine_type
   AND td.ym = r.ym
LEFT JOIN drivers_dedup dd
    ON dd.driver_id = td.driver_id
WHERE r.rn = 1;
