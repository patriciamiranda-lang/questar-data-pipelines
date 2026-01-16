-- 001_create_schemas.sql
-- Cria os schemas base do DW e de administração

CREATE SCHEMA IF NOT EXISTS admin;
CREATE SCHEMA IF NOT EXISTS dw_redshift;

-- Comentários ajudam muito no Query Editor / governança
COMMENT ON SCHEMA admin IS 'Objetos administrativos: procedures, lock tables, metadados de PK/FK.';
COMMENT ON SCHEMA dw_redshift IS 'Data Warehouse (dimensoes, fatos, views e materialized views).';
