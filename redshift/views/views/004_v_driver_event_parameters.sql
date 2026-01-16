CREATE
OR REPLACE VIEW "dw_redshift"."v_driver_event_parameters" AS
SELECT
   ej.driver_id,
   ej.driver_name,
   d.parameter_id,
   d.parameter_name,
   d.parameter_category,
   d.peso_impacto,
   d.acao_recomendada_final,
   count(*) AS qtd_eventos
FROM
   (
      SELECT
         t.driver_id,
         d.driver_name,
         e.scheme_id,
         e.drive_id
      FROM
         dw_redshift.t_events e
         JOIN dw_redshift.t_trips t ON e.drive_id = t.drive_id
         JOIN dw_redshift.d_drivers d ON t.driver_id = d.driver_id
   ) ej
   LEFT JOIN (
      SELECT
         pg_catalog.row_number() OVER() AS parameter_id,
         parametro_base.parametro AS parameter_name,
         parametro_base.categoria AS parameter_category,
         parametro_base.peso_impacto,
         parametro_base.acao_recomendada_final
      FROM
         dw_redshift.parametro_base
   ) d ON 1 = 0
GROUP BY
   ej.driver_id,
   ej.driver_name,
   d.parameter_id,
   d.parameter_name,
   d.parameter_category,
   d.peso_impacto,
   d.acao_recomendada_final
ORDER BY
   ej.driver_id,
   d.parameter_id;
