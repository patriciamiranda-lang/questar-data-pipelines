-- =============================================================
-- Materialized View: dw_redshift.mv_fleet_security_improvement
-- Objetivo: identificar ofensores de segurança por grupo (mapeando schemes -> parâmetro)
-- Retorna, por ofensor, o top driver no grupo e um ranking final por grupo.
-- Refresh: admin.refresh_all_mvs()
-- Dependências:
--   - dw_redshift.t_events
--   - dw_redshift.t_trips
--   - dw_redshift.d_vehicles
--   - dw_redshift.d_groups
--   - dw_redshift.d_schemes
--   - dw_redshift.parametro_base
--   - dw_redshift.d_drivers
-- =============================================================

CREATE SCHEMA IF NOT EXISTS dw_redshift;

DROP MATERIALIZED VIEW IF EXISTS dw_redshift.mv_fleet_security_improvement;

CREATE MATERIALIZED VIEW dw_redshift.mv_fleet_security_improvement
AUTO REFRESH NO
AS
WITH events AS (
    SELECT
        e.event_id,
        e.scheme_id,
        e.vehicle_id,
        t.driver_id
    FROM dw_redshift.t_events e
    JOIN dw_redshift.t_trips t ON e.drive_id = t.drive_id
    WHERE e.scheme_id IS NOT NULL
),
veh AS (
    SELECT
        v.vehicle_id,
        v.group_id,
        g.group_name
    FROM dw_redshift.d_vehicles v
    LEFT JOIN dw_redshift.d_groups g ON v.group_id = g.group_id
),
schemes AS (
    SELECT
        scheme_id,
        REGEXP_REPLACE(
            TRANSLATE(
                LOWER(COALESCE(scheme_description, description, scheme_type, '')),
                'áàãâäéèêëíìîïóòôõöúùûüçñ',
                'aaaaaeeeeiiiiooooouuuucn'
            ),
            '[^a-z0-9]+',
            ''
        ) AS key_txt_scheme
    FROM dw_redshift.d_schemes
),
params AS (
    SELECT
        parametro AS ofensor,
        categoria,
        acao_recomendada_final,
        REGEXP_REPLACE(
            TRANSLATE(
                LOWER(parametro),
                'áàãâäéèêëíìîïóòôõöúùûüçñ',
                'aaaaaeeeeiiiiooooouuuucn'
            ),
            '[^a-z0-9]+',
            ''
        ) AS key_txt_param
    FROM dw_redshift.parametro_base
),
mapped AS (
    SELECT
        v.group_id,
        v.group_name,
        e.driver_id,
        p.ofensor,
        p.categoria,
        p.acao_recomendada_final
    FROM events e
    JOIN schemes s ON e.scheme_id = s.scheme_id
    JOIN veh v     ON e.vehicle_id = v.vehicle_id
    JOIN params p  ON STRPOS(s.key_txt_scheme, p.key_txt_param) > 0
),
driver_agg AS (
    SELECT
        group_id,
        group_name,
        driver_id,
        ofensor,
        categoria,
        acao_recomendada_final,
        COUNT(*) AS total_events
    FROM mapped
    WHERE driver_id IS NOT NULL
    GROUP BY
        group_id,
        group_name,
        driver_id,
        ofensor,
        categoria,
        acao_recomendada_final
),
ranked_driver AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY group_id, ofensor
            ORDER BY total_events DESC
        ) AS rn_driver
    FROM driver_agg
),
final_rank AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY group_id
            ORDER BY total_events DESC
        ) AS ranking
    FROM ranked_driver
    WHERE rn_driver = 1
),
drivers_dedup AS (
    SELECT
        driver_id,
        MAX(driver_name) AS driver_name
    FROM dw_redshift.d_drivers
    GROUP BY driver_id
)
SELECT
    f.group_id,
    f.group_name,
    f.ofensor,
    f.categoria,
    f.total_events,
    f.acao_recomendada_final,
    f.driver_id AS top_driver_id,
    d.driver_name AS top_driver_name,
    f.ranking
FROM final_rank f
LEFT JOIN drivers_dedup d ON f.driver_id = d.driver_id;
