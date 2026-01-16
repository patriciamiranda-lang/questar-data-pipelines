CREATE
OR REPLACE VIEW "dw_redshift"."v_driver_safety_score_monthly" AS
SELECT
    latest.driver_id,
    latest.driver_name,
    latest.group_id,
    latest.group_name,
    latest.ym AS current_month,
    latest.safety_pct,
    latest.prev_pct,
    COALESCE(
        round(latest.safety_pct - latest.prev_pct, 4:: numeric),
        0:: double precision
    ) AS impacto_pct,
    CASE
    WHEN latest.prev_pct IS NULL THEN 'sem histórico':: text
    WHEN latest.safety_pct < latest.prev_pct THEN 'melhorou':: text
    WHEN latest.safety_pct > latest.prev_pct THEN 'piorou':: text
    ELSE 'igual':: text END AS tendencia,
    CASE
    WHEN latest.prev_pct IS NULL THEN 'Motorista sem histórico suficiente para comparação.':: text
    WHEN latest.safety_pct < latest.prev_pct THEN (
        'O motorista melhorou ':: text || round(
            abs(latest.safety_pct - latest.prev_pct),
            2:: numeric
        ):: character varying:: text
    ) || '% em relação ao mês anterior.':: text
    WHEN latest.safety_pct > latest.prev_pct THEN (
        'O motorista piorou ':: text || round(
            abs(latest.safety_pct - latest.prev_pct),
            2:: numeric
        ):: character varying:: text
    ) || '% em relação ao mês anterior.':: text
    ELSE 'O motorista manteve o mesmo nível de segurança em relação ao mês anterior.':: text END AS mensagem,
    pg_catalog.row_number() OVER(
        ORDER BY
            COALESCE(
                latest.safety_pct - latest.prev_pct,
                0:: double precision
            )
    ) AS ranking_melhor,
    pg_catalog.row_number() OVER(
        ORDER BY
            COALESCE(
                latest.safety_pct - latest.prev_pct,
                0:: double precision
            ) DESC
    ) AS ranking_pior
FROM
    (
        SELECT
            ordered.driver_id,
            ordered.driver_name,
            ordered.group_id,
            ordered.group_name,
            ordered.ym,
            ordered.total_points,
            ordered.total_km,
            ordered.safety_pct,
            ordered.prev_pct
        FROM
            (
                SELECT
                    agg.driver_id,
                    agg.driver_name,
                    agg.group_id,
                    agg.group_name,
                    agg.ym,
                    agg.total_points,
                    agg.total_km,
                    agg.safety_pct,
                    lead(agg.safety_pct) OVER(
                        PARTITION BY agg.driver_id
                        ORDER BY
                            agg.ym DESC
                    ) AS prev_pct
                FROM
                    (
                        SELECT
                            base.driver_id,
                            base.driver_name,
                            base.group_id,
                            base.group_name,
                            base.ym,
                            sum(base.safety_points) AS total_points,
                            sum(base.km) AS total_km,
                            CASE
                            WHEN sum(base.km) > 0:: double precision THEN sum(base.safety_points):: double precision / sum(base.km) * 100:: double precision
                            ELSE NULL:: double precision END AS safety_pct
                        FROM
                            (
                                SELECT
                                    t.driver_id,
                                    d.driver_name,
                                    v.group_id,
                                    g.group_name,
                                    date_trunc('month':: text, t.end_drive):: date AS ym,
                                    COALESCE(t.turn1, 0) + COALESCE(t.turn2, 0) + COALESCE(t.turn3, 0) + COALESCE(t.break1, 0) + COALESCE(t.break2, 0) + COALESCE(t.break3, 0) + COALESCE(t.acceleration1, 0) + COALESCE(t.acceleration2, 0) + COALESCE(t.acceleration3, 0) + COALESCE(t.speed_road1, 0) + COALESCE(t.speed_road2, 0) + COALESCE(t.speed_road3, 0) AS safety_points,
                                    COALESCE(t.mileage, 0:: double precision) AS km
                                FROM
                                    dw_redshift.t_trips t
                                    JOIN dw_redshift.d_vehicles v ON v.vehicle_id = t.vehicle_id
                                    JOIN dw_redshift.d_drivers d ON d.driver_id = t.driver_id
                                    JOIN dw_redshift.d_groups g ON g.group_id = v.group_id
                                WHERE
                                    t.end_drive >= date_add(
                                        'month':: text,
                                        -2:: bigint,
                                        'now':: text:: date:: timestamp without time zone
                                    )
                            ) base
                        GROUP BY
                            base.driver_id,
                            base.driver_name,
                            base.group_id,
                            base.group_name,
                            base.ym
                    ) agg
            ) ordered
        WHERE
            ordered.ym = (
                (
                    SELECT
                        "max"(ordered.ym) AS "max"
                    FROM
                        (
                            SELECT
                                agg.driver_id,
                                agg.driver_name,
                                agg.group_id,
                                agg.group_name,
                                agg.ym,
                                agg.total_points,
                                agg.total_km,
                                agg.safety_pct,
                                lead(agg.safety_pct) OVER(
                                    PARTITION BY agg.driver_id
                                    ORDER BY
                                        agg.ym DESC
                                ) AS prev_pct
                            FROM
                                (
                                    SELECT
                                        base.driver_id,
                                        base.driver_name,
                                        base.group_id,
                                        base.group_name,
                                        base.ym,
                                        sum(base.safety_points) AS total_points,
                                        sum(base.km) AS total_km,
                                        CASE
                                        WHEN sum(base.km) > 0:: double precision THEN sum(base.safety_points):: double precision / sum(base.km) * 100:: double precision
                                        ELSE NULL:: double precision END AS safety_pct
                                    FROM
                                        (
                                            SELECT
                                                t.driver_id,
                                                d.driver_name,
                                                v.group_id,
                                                g.group_name,
                                                date_trunc('month':: text, t.end_drive):: date AS ym,
                                                COALESCE(t.turn1, 0) + COALESCE(t.turn2, 0) + COALESCE(t.turn3, 0) + COALESCE(t.break1, 0) + COALESCE(t.break2, 0) + COALESCE(t.break3, 0) + COALESCE(t.acceleration1, 0) + COALESCE(t.acceleration2, 0) + COALESCE(t.acceleration3, 0) + COALESCE(t.speed_road1, 0) + COALESCE(t.speed_road2, 0) + COALESCE(t.speed_road3, 0) AS safety_points,
                                                COALESCE(t.mileage, 0:: double precision) AS km
                                            FROM
                                                dw_redshift.t_trips t
                                                JOIN dw_redshift.d_vehicles v ON v.vehicle_id = t.vehicle_id
                                                JOIN dw_redshift.d_drivers d ON d.driver_id = t.driver_id
                                                JOIN dw_redshift.d_groups g ON g.group_id = v.group_id
                                            WHERE
                                                t.end_drive >= date_add(
                                                    'month':: text,
                                                    -2:: bigint,
                                                    'now':: text:: date:: timestamp without time zone
                                                )
                                        ) base
                                    GROUP BY
                                        base.driver_id,
                                        base.driver_name,
                                        base.group_id,
                                        base.group_name,
                                        base.ym
                                ) agg
                        ) ordered
                )
            )
    ) latest;
