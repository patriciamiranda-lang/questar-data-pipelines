-- =============================================================
-- Materialized View: dw_redshift.mv_eventos_pedal_com_grupo
-- Objetivo: consolidar eventos relacionados a "pedal" por motorista/grupo/mês
-- Refresh: admin.refresh_all_mvs()
-- Dependências:
--   - dw_redshift.t_events
--   - dw_redshift.t_trips
--   - dw_redshift.d_drivers
--   - dw_redshift.d_vehicles
--   - dw_redshift.d_groups
--   - dw_redshift.d_schemes
--   - dw_redshift.parametro_base
-- =============================================================

CREATE SCHEMA IF NOT EXISTS dw_redshift;

DROP MATERIALIZED VIEW IF EXISTS dw_redshift.mv_eventos_pedal_com_grupo;

CREATE MATERIALIZED VIEW dw_redshift.mv_eventos_pedal_com_grupo
SORTKEY (driver_id, year, month)
AS
WITH base AS (
    SELECT
        t.driver_id,
        d.driver_name,
        g.group_name,
        CASE
            WHEN s.description ILIKE '%embreagem%' THEN 'Falha na embreagem'
            WHEN s.description ILIKE '%accelerator%' OR s.description ILIKE '%acelerador%' THEN 'Uso excessivo de potência'
            WHEN s.description ILIKE '%brake%' OR s.description ILIKE '%freio%' THEN 'Falha no freio/ABS'
            ELSE 'Outros'
        END AS parametro,
        EXTRACT(YEAR  FROM e.start_time)::INT AS year,
        EXTRACT(MONTH FROM e.start_time)::INT AS month
    FROM dw_redshift.t_events e
    JOIN dw_redshift.t_trips t     ON t.drive_id  = e.drive_id
    JOIN dw_redshift.d_drivers d   ON d.driver_id = t.driver_id
    JOIN dw_redshift.d_vehicles v  ON v.vehicle_id = e.vehicle_id
    LEFT JOIN dw_redshift.d_groups g  ON g.group_id = v.group_id
    LEFT JOIN dw_redshift.d_schemes s ON s.scheme_id = e.scheme_id
    WHERE s.description ILIKE '%pedal%'
)
SELECT
    b.driver_id,
    b.driver_name,
    b.group_name,
    b.parametro,
    pb.categoria,
    pb.peso_impacto,
    pb.acao_recomendada_final,
    b.year,
    b.month,
    COUNT(*) AS qtd_eventos,
    COUNT(*) * pb.peso_impacto AS impacto_total
FROM base b
LEFT JOIN dw_redshift.parametro_base pb ON pb.parametro = b.parametro
GROUP BY
    b.driver_id,
    b.driver_name,
    b.group_name,
    b.parametro,
    pb.categoria,
    pb.peso_impacto,
    pb.acao_recomendada_final,
    b.year,
    b.month;
