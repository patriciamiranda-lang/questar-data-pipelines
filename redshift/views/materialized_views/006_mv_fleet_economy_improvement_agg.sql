-- =============================================================
-- Materialized View: dw_redshift.mv_fleet_economy_improvement_agg
-- Objetivo: identificar ofensores de economia (agregado) por grupo, com top driver
-- Refresh: admin.refresh_all_mvs()
-- Dependências:
--   - dw_redshift.t_trips
--   - dw_redshift.d_vehicles
--   - dw_redshift.d_groups
--   - dw_redshift.d_drivers
-- =============================================================

CREATE SCHEMA IF NOT EXISTS dw_redshift;

DROP MATERIALIZED VIEW IF EXISTS dw_redshift.mv_fleet_economy_improvement_agg;

CREATE MATERIALIZED VIEW dw_redshift.mv_fleet_economy_improvement_agg
AUTO REFRESH NO
AS
WITH trips AS (
    SELECT
        t.vehicle_id,
        t.driver_id,
        v.group_id,
        g.group_name,
        t.engine,
        t.speed_road3,
        t.acceleration3,
        t.idle_duration,
        t.gearbox
    FROM dw_redshift.t_trips t
    LEFT JOIN dw_redshift.d_vehicles v ON t.vehicle_id = v.vehicle_id
    LEFT JOIN dw_redshift.d_groups g   ON v.group_id = g.group_id
),
driver_agg AS (
    SELECT
        group_id,
        group_name,
        driver_id,
        SUM(engine)        AS engine_events,
        SUM(speed_road3)   AS speeding_events,
        SUM(acceleration3) AS accel_events,
        SUM(idle_duration) AS idle_events,
        SUM(gearbox)       AS gearbox_events
    FROM trips
    WHERE group_id IS NOT NULL
      AND driver_id IS NOT NULL
    GROUP BY
        group_id,
        group_name,
        driver_id
),
unpivoted_events AS (
    SELECT
        group_id,
        group_name,
        driver_id,
        'Uso excessivo de potência' AS ofensor,
        engine_events AS total_events,
        'Troque as marchas dentro da faixa verde e evite aceleração prolongada no pedal máximo.' AS action_recommended
    FROM driver_agg
    WHERE engine_events > 0

    UNION ALL
    SELECT
        group_id,
        group_name,
        driver_id,
        'Excesso de velocidade tracionando',
        speeding_events,
        'Reduza a velocidade antes das curvas e mantenha tração dentro dos limites definidos.'
    FROM driver_agg
    WHERE speeding_events > 0

    UNION ALL
    SELECT
        group_id,
        group_name,
        driver_id,
        'Aceleração brusca',
        accel_events,
        'Acelere progressivamente e mantenha condução suave para reduzir consumo.'
    FROM driver_agg
    WHERE accel_events > 0

    UNION ALL
    SELECT
        group_id,
        group_name,
        driver_id,
        'Ociosidade',
        idle_events,
        'Desligue o motor em paradas prolongadas para evitar consumo desnecessário.'
    FROM driver_agg
    WHERE idle_events > 0

    UNION ALL
    SELECT
        group_id,
        group_name,
        driver_id,
        'Uso excessivo de transmissão',
        gearbox_events,
        'Evite trocar marchas fora da rotação ideal e mantenha operação suave.'
    FROM driver_agg
    WHERE gearbox_events > 0
),
ranked AS (
    SELECT
        group_id,
        group_name,
        driver_id,
        ofensor,
        total_events,
        action_recommended,
        ROW_NUMBER() OVER (
            PARTITION BY group_id, ofensor
            ORDER BY total_events DESC
        ) AS rn
    FROM unpivoted_events
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
    r.driver_id AS top_driver_id,
    d.driver_name AS top_driver_name,
    r.ofensor,
    r.total_events,
    r.action_recommended,
    ROW_NUMBER() OVER (
        PARTITION BY r.group_id
        ORDER BY r.total_events DESC
    ) AS ranking
FROM ranked r
LEFT JOIN drivers_dedup d ON r.driver_id = d.driver_id
WHERE r.rn = 1;
