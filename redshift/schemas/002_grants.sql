-- 002_grants.sql
-- Permissões de acesso e default privileges

-- Troque pelos nomes reais do seu ambiente
-- Exemplo:
--   ROLE/GRUPO leitura:  role_dw_read
--   ROLE/GRUPO escrita:  role_dw_rw
--   OWNER padrão:        seu usuário/role de deploy

-- 1) USAGE nos schemas
GRANT USAGE ON SCHEMA dw_redshift TO ROLE <<ROLE_DW_READ>>;
GRANT USAGE ON SCHEMA dw_redshift TO ROLE <<ROLE_DW_RW>>;
GRANT USAGE ON SCHEMA admin      TO ROLE <<ROLE_DW_RW>>;

-- 2) Permissões nas tabelas já existentes
GRANT SELECT ON ALL TABLES IN SCHEMA dw_redshift TO ROLE <<ROLE_DW_READ>>;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dw_redshift TO ROLE <<ROLE_DW_RW>>;

-- Se quiser proteger o schema admin (geralmente só RW)
GRANT SELECT ON ALL TABLES IN SCHEMA admin TO ROLE <<ROLE_DW_RW>>;

-- 3) Views / Materialized Views (no Redshift, entram como relations também)
GRANT SELECT ON ALL TABLES IN SCHEMA dw_redshift TO ROLE <<ROLE_DW_READ>>;
GRANT SELECT ON ALL TABLES IN SCHEMA dw_redshift TO ROLE <<ROLE_DW_RW>>;

-- 4) Stored procedures (quem pode EXECUTE)
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA admin TO ROLE <<ROLE_DW_RW>>;

-- 5) DEFAULT PRIVILEGES (o mais importante para não ter surpresa no futuro)
-- Atenção: DEFAULT PRIVILEGES aplicam para objetos criados POR UM OWNER específico.
-- Então rode isso com o usuário/role que cria as tabelas/views/procs (o "deploy user").

ALTER DEFAULT PRIVILEGES IN SCHEMA dw_redshift
GRANT SELECT ON TABLES TO ROLE <<ROLE_DW_READ>>;

ALTER DEFAULT PRIVILEGES IN SCHEMA dw_redshift
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ROLE <<ROLE_DW_RW>>;

ALTER DEFAULT PRIVILEGES IN SCHEMA admin
GRANT EXECUTE ON PROCEDURES TO ROLE <<ROLE_DW_RW>>;

-- (Opcional) Se vocês usam sequences (identity costuma cuidar, mas pode ser útil)
ALTER DEFAULT PRIVILEGES IN SCHEMA dw_redshift
GRANT USAGE ON SEQUENCES TO ROLE <<ROLE_DW_RW>>;

