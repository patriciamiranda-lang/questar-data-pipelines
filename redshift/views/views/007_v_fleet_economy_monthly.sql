CREATE
OR REPLACE VIEW "dw_redshift"."v_fleet_economy_monthly" AS
SELECT
        joined."year",
        joined."month",
        joined.group_id,
        joined.group_name,
        joined.vehicle_engine_type,
        joined.total_km,
        joined.total_energy,
        CASE
        WHEN joined.total_energy IS NULL
        OR joined.total_energy = 0:: double precision THEN NULL:: double precision
        ELSE joined.total_km / joined.total_energy END AS avg_economy
FROM
        (
                SELECT
                        tr."year",
                        tr."month",
                        ve.group_id,
                        gr.group_name,
                        ve.vehicle_engine_type,
                        sum(tr.total_km) AS total_km,
                        sum(tr.total_energy) AS total_energy
                FROM
                        (
                                SELECT
                                        t.drive_id,
                                        t.vehicle_id,
                                        t.mileage AS total_km,
                                        t.energy_used AS total_energy,
                                        pgdate_part('year':: character varying:: text, t.end_drive) AS "year",
                                        pgdate_part('month':: character varying:: text, t.end_drive) AS "month"
                                FROM
                                        dw_redshift.t_trips t
                                WHERE
                                        t.end_drive IS NOT NULL
                        ) tr
                        JOIN (
                                SELECT
                                        v.vehicle_id,
                                        v.group_id,
                                        v.vehicle_engine_type
                                FROM
                                        dw_redshift.d_vehicles v
                        ) ve ON tr.vehicle_id = ve.vehicle_id
                        LEFT JOIN (
                                SELECT
                                        g.group_id,
                                        g.group_name
                                FROM
                                        dw_redshift.d_groups g
                        ) gr ON ve.group_id = gr.group_id
                GROUP BY
                        tr."year",
                        tr."month",
                        ve.group_id,
                        gr.group_name,
                        ve.vehicle_engine_type
        ) joined;
