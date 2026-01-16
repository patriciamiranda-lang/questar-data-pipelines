CREATE
OR REPLACE VIEW dev.dw_redshift.v_driver_keymap AS
SELECT
    d.driver_id:: varchar AS driver_tech_id,
    d.driver_id:: varchar AS driver_id,
    MAX(TRIM(d.driver_name)) AS driver_name,
    MAX(d.group_id) AS group_id,
    MAX(g.group_name) AS group_name
FROM
    dev.dw_redshift.d_drivers d
    LEFT JOIN dev.dw_redshift.d_groups g ON d.group_id = g.group_id
GROUP BY
    d.driver_id WITH NO SCHEMA BINDING;
