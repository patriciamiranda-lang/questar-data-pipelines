-- =============================================================
-- Procedure: admin.sp_build_pkfk_dictionary(target_schema varchar)
-- Objetivo: popular admin.key_registry com PKs e FKs do schema alvo
-- Fonte: information_schema (constraints já existentes no Redshift)
-- =============================================================

CREATE SCHEMA IF NOT EXISTS admin;

CREATE OR REPLACE PROCEDURE admin.sp_build_pkfk_dictionary(target_schema character varying(256))
LANGUAGE plpgsql
AS $$
DECLARE
    r RECORD;
BEGIN
    -- Limpa entradas existentes do schema alvo
    DELETE FROM admin.key_registry WHERE schema_name = target_schema;

    -- PKs
    INSERT INTO admin.key_registry(schema_name, table_name, column_name, constraint_type)
    SELECT table_schema, table_name, column_name, 'PK'
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND table_schema = target_schema;

    -- FKs
    INSERT INTO admin.key_registry(
        schema_name, table_name, column_name,
        ref_schema_name, ref_table_name, ref_column_name,
        constraint_type
    )
    SELECT DISTINCT
        tc.table_schema, tc.table_name, kcu.column_name,
        ccu.table_schema AS ref_schema_name,
        ccu.table_name   AS ref_table_name,
        ccu.column_name  AS ref_column_name,
        'FK'
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
         ON tc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage ccu
         ON ccu.constraint_name = tc.constraint_name
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_schema = target_schema;
END;
$$;
