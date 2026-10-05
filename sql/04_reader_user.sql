-- 04_reader_user.sql: read-only login for Databricks. Run once as olist_admin.
-- Usage: psql "$PGCONN" -v reader_pw="..." -f sql/04_reader_user.sql
CREATE ROLE dbx_reader LOGIN PASSWORD :'reader_pw';
GRANT CONNECT ON DATABASE olist TO dbx_reader;
GRANT USAGE ON SCHEMA app, ref TO dbx_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA app, ref TO dbx_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA app, ref GRANT SELECT ON TABLES TO dbx_reader;
