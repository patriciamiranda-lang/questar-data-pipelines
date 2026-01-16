CREATE
OR REPLACE VIEW dev.dw_redshift.v_event_parameter_dictionary_base AS
SELECT
    DISTINCT parametro,
    categoria,
    peso_impacto:: DOUBLE PRECISION AS peso_impacto,
    acao_recomendada_final
FROM
    dev.dw_redshift.parametro_base WITH NO SCHEMA BINDING;
