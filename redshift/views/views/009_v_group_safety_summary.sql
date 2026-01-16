CREATE
OR REPLACE VIEW "dw_redshift"."v_group_safety_summary" AS
SELECT
    ranked.group_id,
    ranked.group_name,
    ranked."year" AS current_year,
    ranked."month" AS current_month,
    ranked.total_points,
    ranked.total_km,
    ranked.safety_pct,
    ranked.safety_pct_prev,
    md.driver_name AS driver_referencia,
    CASE
    WHEN ranked.safety_pct_prev IS NULL THEN 'sem histórico':: text
    WHEN ranked.safety_pct < ranked.safety_pct_prev THEN 'melhorou':: text
    WHEN ranked.safety_pct > ranked.safety_pct_prev THEN 'piorou':: text
    ELSE 'igual':: text END AS safety_tendencia,
    CASE
    WHEN ranked.safety_pct_prev IS NULL THEN 'Sem histórico anterior para comparação.':: text
    WHEN ranked.safety_pct < ranked.safety_pct_prev THEN (
        (
            (
                'A segurança da frota melhorou ':: text || round(
                    abs(ranked.safety_pct - ranked.safety_pct_prev),
                    2:: numeric
                ):: text
            ) || '% em relação ao mês anterior. Motorista de referência: ':: text
        ) || md.driver_name:: text
    ) || '.':: text
    WHEN ranked.safety_pct > ranked.safety_pct_prev THEN (
        (
            (
                'A segurança da frota piorou ':: text || round(
                    abs(ranked.safety_pct - ranked.safety_pct_prev),
                    2:: numeric
                ):: text
            ) || '% em relação ao mês anterior. Motorista de referência: ':: text
        ) || md.driver_name:: text
    ) || '.':: text
    ELSE 'A segurança da frota permaneceu estável em relação ao mês anterior.':: text END AS safety_message
FROM
    (
        SELECT
            base.group_id,
            base.group_name,
            base."year",
            base."month",
            base.ym,
            base.total_points,
            base.total_km,
            base.safety_pct,
            lead(base.safety_pct) OVER(
                PARTITION BY base.group_id
                ORDER BY
                    base."year" DESC,
                    base."month" DESC
            ) AS safety_pct_prev,
            pg_catalog.row_number() OVER(
                PARTITION BY base.group_id
                ORDER BY
                    base."year" DESC,
                    base."month" DESC
            ) AS rn
        FROM
            dw_redshift.mv_group_safety_monthly_base base
    ) ranked
    LEFT JOIN dw_redshift.mv_group_main_driver_monthly md ON md.group_id = ranked.group_id
    AND md.ym = ranked.ym
    AND md.rn = 1
WHERE
    ranked.rn = 1;
