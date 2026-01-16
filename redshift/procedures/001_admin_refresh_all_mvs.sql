CREATE OR REPLACE PROCEDURE "admin".refresh_all_mvs()
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $$
DECLARE
    v_running BOOLEAN;
BEGIN
    -- Verifica lock
    SELECT is_running
    INTO v_running
    FROM admin.mv_refresh_lock
    WHERE lock_id = 1;

    IF v_running THEN
        RAISE NOTICE 'Refresh de MVs já está em execução. Abortando.';
        RETURN;
    END IF;

    -- Ativa lock
    UPDATE admin.mv_refresh_lock
    SET is_running = true,
        start_time = CURRENT_TIMESTAMP
    WHERE lock_id = 1;

    -- REFRESH DAS MVs
    REFRESH MATERIALIZED VIEW dw_redshift.mv_driver_errors;                           
    REFRESH MATERIALIZED VIEW dw_redshift.mv_driver_top_offenders;
    REFRESH MATERIALIZED VIEW dw_redshift.mv_eventos_pedal_com_grupo;
    REFRESH MATERIALIZED VIEW dw_redshift.mv_fleet_economy_comparison;  
    REFRESH MATERIALIZED VIEW dw_redshift.mv_fleet_economy_driver_contribution;
    REFRESH MATERIALIZED VIEW dw_redshift.mv_fleet_economy_improvement_agg;
    REFRESH MATERIALIZED VIEW dw_redshift.mv_fleet_safety_monthly_agg;
    REFRESH MATERIALIZED VIEW dw_redshift.mv_fleet_security_improvement;
    REFRESH MATERIALIZED VIEW dw_redshift.mv_group_main_driver_monthly;
    REFRESH MATERIALIZED VIEW dw_redshift.mv_group_safety_monthly_base;
    REFRESH MATERIALIZED VIEW dw_redshift.mv_ranking_motoristas;
    REFRESH MATERIALIZED VIEW dw_redshift.mv_vehicle_health_com_grupo;

    -- Libera o lock
    UPDATE admin.mv_refresh_lock
    SET is_running = false,
        end_time = CURRENT_TIMESTAMP
    WHERE lock_id = 1;
END;
$$
