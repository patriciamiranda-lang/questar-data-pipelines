CREATE
OR REPLACE VIEW dev.dw_redshift.v_driver_economy_monthly AS WITH trips AS (
    SELECT
        EXTRACT(
            YEAR
            FROM
                t.start_drive
        ):: INT AS year,
        EXTRACT(
            MONTH
            FROM
                t.start_drive
        ):: INT AS month,
        v.group_id,
        g.group_name,
        t.driver_id AS fk_driver,
        t.driver_id:: VARCHAR AS driver_id,
        v.vehicle_engine_type,
        CASE
        WHEN LOWER(v.vehicle_engine_type) IN ('diesel', 'gasoline', 'etanol', 'flex') THEN 'Combustão'
        WHEN LOWER(v.vehicle_engine_type) IN ('electric', 'ev', 'elétrico', 'eletrico') THEN 'Elétrico'
        WHEN LOWER(v.vehicle_engine_type) IN ('cng', 'gnv', 'lng', 'gas') THEN 'Gás'
        ELSE 'Desconhecido' END AS energy_type,
        SUM(COALESCE(t.mileage, 0)) AS total_km,
        SUM(COALESCE(t.energy_used, 0)) AS total_energy
    FROM
        dev.dw_redshift.t_trips t
        LEFT JOIN dev.dw_redshift.d_vehicles v ON v.vehicle_id = t.vehicle_id
        LEFT JOIN dev.dw_redshift.d_groups g ON g.group_id = v.group_id
    WHERE
        t.start_drive IS NOT NULL
    GROUP BY
        EXTRACT(
            YEAR
            FROM
                t.start_drive
        ),
        EXTRACT(
            MONTH
            FROM
                t.start_drive
        ),
        v.group_id,
        g.group_name,
        t.driver_id,
        v.vehicle_engine_type
)
SELECT
    t.year,
    t.month,
    t.group_id,
    t.group_name,
    t.driver_id,
    COALESCE(km.driver_name, '(sem nome)') AS driver_name,
    t.vehicle_engine_type,
    t.energy_type,
    t.total_km,
    t.total_energy,
    CASE
    WHEN t.total_energy > 0 THEN t.total_km / t.total_energy
    ELSE NULL END AS avg_economy
FROM
    trips t
    LEFT JOIN dev.dw_redshift.v_driver_keymap km ON km.driver_id = t.driver_id WITH NO SCHEMA BINDING;
