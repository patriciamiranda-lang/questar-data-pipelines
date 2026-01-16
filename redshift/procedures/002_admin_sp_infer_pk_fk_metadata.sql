-- =============================================================
-- Procedure: admin.sp_infer_pk_fk_metadata(target_schema varchar)
-- Objetivo: inferir/popular metadados de PK/FK do schema alvo
-- Implementação: chama admin.sp_build_pkfk_dictionary(target_schema)
-- =============================================================

CREATE SCHEMA IF NOT EXISTS admin;

CREATE OR REPLACE PROCEDURE admin.sp_infer_pk_fk_metadata(target_schema character varying(256))
LANGUAGE plpgsql
AS $$
BEGIN
    CALL admin.sp_build_pkfk_dictionary(target_schema);
END;
$$;
