-- =============================================================
-- Procedure: admin.sp_apply_pk_fk_constraints()
-- Objetivo: aplicar PK/FK no Redshift a partir do registro em admin.key_registry
-- Observação: útil para padronizar constraints como metadado/documentação
-- =============================================================

CREATE SCHEMA IF NOT EXISTS admin;

CREATE OR REPLACE PROCEDURE admin.sp_apply_pk_fk_constraints()
LANGUAGE plpgsql
AS $$
DECLARE
    r RECORD;
    sql_cmd VARCHAR;
BEGIN
    -- PKs
    FOR r IN
        SELECT schema_name, table_name, column_name
        FROM admin.key_registry
        WHERE constraint_type = 'PK'
    LOOP
        sql_cmd := 'ALTER TABLE '
            || r.schema_name || '.' || r.table_name
            || ' ADD CONSTRAINT pk_' || r.table_name || '_' || r.column_name
            || ' PRIMARY KEY (' || r.column_name || ');';

        RAISE INFO 'Executing: %', sql_cmd;

        EXECUTE sql_cmd;
    END LOOP;

    -- FKs
    FOR r IN
        SELECT schema_name, table_name, column_name, ref_schema_name, ref_table
