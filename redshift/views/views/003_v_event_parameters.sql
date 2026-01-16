CREATE
OR REPLACE VIEW dev.dw_redshift.v_event_parameters AS
SELECT
    ROW_NUMBER() OVER () AS parameter_id,
    parametro AS parameter_name,
    categoria AS parameter_category,
    categoria,
NULL AS
    parent_scheme,
NULL AS
    parameter_description,
    peso_impacto,
    acao_recomendada_final,
NULL AS
    explanation_final,
NULL AS
    example_good,
NULL AS
    example_bad
FROM
    dev.dw_redshift.parametro_base WITH NO SCHEMA BINDING;
