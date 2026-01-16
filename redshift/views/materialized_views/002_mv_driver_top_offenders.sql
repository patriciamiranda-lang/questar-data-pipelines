-- =============================================================
-- Materialized View: dw_redshift.mv_driver_top_offenders
-- Objetivo: Top 5 ofensores (parâmetros) por motorista por mês
-- Refresh: admin.refresh_all_mvs()
-- Dependências:
--   - dw_redshift.t_trips
--   - dw_redshift.t_events
--   - dw_redshift.d_drivers
--   - dw_redshift.d_schemes
--   - dw_redshift.d_event_dictionary
-- =============================================================

CREATE SCHEMA IF NOT EXISTS dw_redshift;

DROP MATERIALIZED VIEW IF EXISTS dw_redshift.mv_driver_top_offenders;

CREATE MATERIALIZED VIEW dw_redshift.mv_driver_top_offenders AS
WITH base_trips AS (
    SELECT
        t.drive_id,
        t.driver_id,
        DATE_TRUNC('month', t.start_drive)::DATE AS ym
    FROM dw_redshift.t_trips t
    WHERE t.start_drive IS NOT NULL
),
base_events_raw AS (
    SELECT
        t.driver_id,
        t.ym,
        e.scheme_id
    FROM dw_redshift.t_events e
    INNER JOIN base_trips t ON e.drive_id = t.drive_id
),
base_with_desc AS (
    SELECT
        t.driver_id::VARCHAR AS driver_id,
        COALESCE(d.driver_name, '(sem nome)') AS driver_name,
        t.ym,
        LOWER(
            translate(
                COALESCE(s.scheme_description, ''),
                'ÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑáàãâäéèêëíìîïóòôõöúùûüçñ',
                'AAAAAEEEEIIIIOOOOOUUUUCNaaaaaeeeeiiiiooooouuuucn'
            )
        ) AS txt
    FROM base_events_raw t
    LEFT JOIN dw_redshift.d_drivers d ON t.driver_id = d.driver_id
    LEFT JOIN dw_redshift.d_schemes s ON t.scheme_id = s.scheme_id
),
mapped AS (
    SELECT
        driver_id,
        driver_name,
        ym,
        CASE
            WHEN txt LIKE '%aceleracao%' THEN 'Aceleração Brusca'
            WHEN txt LIKE '%curva%' THEN 'Curva Brusca'
            WHEN txt LIKE '%fread%' THEN 'Freada Brusca'
            WHEN txt LIKE '%poschave%' THEN 'Dirigir com pós-chave desligado'
            WHEN txt LIKE '%fadiga%' THEN 'Fadiga'
            WHEN txt LIKE '%overspeed%' OR txt LIKE '%speeding%' THEN 'Excesso de velocidade em pista seca'
            WHEN txt LIKE '%banguela%' THEN 'Dirigir na banguela'
            WHEN txt LIKE '%pedaldefreio%' THEN 'Excesso de uso do pedal de freio >30 km/h'
            WHEN txt LIKE '%abs%' THEN 'ABS acionado'
            WHEN txt LIKE '%freiodemao%' THEN 'Estacionar sem freio de mão'
            ELSE NULL
        END AS parametro
    FROM base_with_desc
    WHERE
        CASE
            WHEN txt LIKE '%aceleracao%' THEN TRUE
            WHEN txt LIKE '%curva%' THEN TRUE
            WHEN txt LIKE '%fread%' THEN TRUE
            WHEN txt LIKE '%poschave%' THEN TRUE
            WHEN txt LIKE '%fadiga%' THEN TRUE
            WHEN txt LIKE '%overspeed%' OR txt LIKE '%speeding%' THEN TRUE
            WHEN txt LIKE '%banguela%' THEN TRUE
            WHEN txt LIKE '%pedaldefreio%' THEN TRUE
            WHEN txt LIKE '%abs%' THEN TRUE
            WHEN txt LIKE '%freiodemao%' THEN TRUE
            ELSE FALSE
        END
),
joined_and_agg AS (
    SELECT
        m.driver_id,
        m.driver_name,
        d.categoria,
        d.parametro,
        d.peso_impacto AS peso_impacto_final,
        MAX(d.acao_recomendada_final) AS acao_recomendada_final,
        EXTRACT(YEAR  FROM m.ym)::INT AS year,
        EXTRACT(MONTH FROM m.ym)::INT AS month,
        COUNT(*) AS total_eventos,
        COUNT(*) * d.peso_impacto AS pontuacao_impacto_final
    FROM mapped m
    JOIN dw_redshift.d_event_dictionary d ON m.parametro = d.parametro
    GROUP BY
        m.driver_id,
        m.driver_name,
        d.categoria,
        d.parametro,
        year,
        month,
        d.peso_impacto
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY driver_id, year, month
            ORDER BY pontuacao_impacto_final DESC, total_eventos DESC
        ) AS ranking_parametro
    FROM joined_and_agg
)
SELECT
    driver_id,
    driver_name,
    categoria,
    parametro,
    year,
    month,
    total_eventos,
    peso_impacto_final,
    pontuacao_impacto_final,
    acao_recomendada_final,
    ranking_parametro
FROM ranked
WHERE ranking_parametro <= 5;
