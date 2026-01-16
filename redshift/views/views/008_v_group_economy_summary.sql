CREATE
OR REPLACE VIEW dev.dw_redshift.v_group_economy_summary AS
SELECT
    group_id,
    group_name,
    ROUND(AVG(avg_economy), 2) AS media_economia_grupo,
    'Sua média de economia para a frota ' || group_name || ' é de ' || TO_CHAR(ROUND(AVG(avg_economy), 2), 'FM999990.00') || ' km/l.' AS mensagem_economia
FROM
    dev.dw_redshift.v_fleet_economy_monthly
WHERE
    avg_economy IS NOT NULL
GROUP BY
    group_id,
    group_name
ORDER BY
    media_economia_grupo DESC WITH NO SCHEMA BINDING;
