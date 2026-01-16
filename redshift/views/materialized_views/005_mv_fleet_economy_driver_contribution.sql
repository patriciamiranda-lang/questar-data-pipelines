-- =============================================================
-- Materialized View: dw_redshift.mv_fleet_economy_driver_contribution
-- Objetivo: medir contribuição de cada motorista vs média da frota (grupo + tipo motor)
-- Retorna apenas o mês mais recente por (group_id, vehicle_engine_type).
-- Refresh: admin.refresh_all_mvs()
-- Dependências:
--   - dw_redshift.t_trips
--   - dw_redshift.d_vehicles
--   - dw_redshift.d_groups
--   - dw_redshift.d_drivers
-- =============================================================

CREATE SCHEMA IF NOT EXISTS dw_redshift;

DROP MATERIALIZED VIEW IF EXISTS dw_redshift.mv_fleet_economy_driver_contribution;

CREATE MATERIALIZED VIEW dw_redshift.mv_fleet_economy_driver_contribution
AUTO REFRESH NO
AS
WITH driver_base AS (
    SELECT
        v.group_id,
        g.group_name,
        v.vehicle_engine_type,
        DATE_TRUNC('month', t.end_drive)::date AS ym,
        t.driver_id,
        SUM(COALESCE(t.mileage, 0)) AS total_km,
        SUM(COALESCE(t.energy_used, 0)) AS total_energy,
        CASE
            WHEN v.vehicle_engine_type = 'Elétrico' AND SUM(t.mileage) > 0 THEN SUM(t.energy_used) / SUM(t.mileage)
            WHEN SUM(t.energy_used) > 0 THEN SUM(t.mileage) / SUM(t.energy_used)
            ELSE NULL
        END AS driver_avg_economy
    FROM dw_redshift.t_trips t
    JOIN dw_redshift.d_vehicles v ON v.vehicle_id = t.vehicle_id
    JOIN dw_redshift.d_groups g   ON g.group_id = v.group_id
    WHERE t.end_drive IS NOT NULL
      AND t.driver_id IS NOT NULL
    GROUP BY
        v.group_id,
        g.group_name,
        v.vehicle_engine_type,
        DATE_TRUNC('month', t.end_drive),
        t.driver_id
),
fleet_avg AS (
    SELECT
        group_id,
        vehicle_engine_type,
        ym,
        AVG(driver_avg_economy) AS fleet_avg_economy
    FROM driver_base
    WHERE driver_avg_economy IS NOT NULL
    GROUP BY
        group_id,
        vehicle_engine_type,
        ym
),
joined AS (
    SELECT
        d.group_id,
        d.group_name,
        d.vehicle_engine_type,
        d.ym,
        d.driver_id,
        d.driver_avg_economy,
        f.fleet_avg_economy,
        ROUND(d.driver_avg_economy - f.fleet_avg_economy, 4) AS economy_delta
    FROM driver_base d
    JOIN fleet_avg f
      ON f.group_id = d.group_id
     AND f.vehicle_engine_type = d.vehicle_engine_type
     AND f.ym = d.ym
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY group_id, vehicle_engine_type
            ORDER BY ym DESC
        ) AS rn
    FROM joined
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
    r.vehicle_engine_type,
    r.ym,
    r.driver_id,
    d.driver_name,
    r.driver_avg_economy,
    r.fleet_avg_economy,
    r.economy_delta,
    CASE
        WHEN r.economy_delta > 0 THEN 'Contribuiu positivamente para a economia da frota.'
        WHEN r.economy_delta < 0 THEN 'Contribuiu negativamente para a economia da frota.'
        ELSE 'Contribuição neutra.'
    END AS impacto_economia
FROM ranked r
LEFT JOIN drivers_dedup d ON d.driver_id = r.driver_id
WHERE r.rn = 1;
