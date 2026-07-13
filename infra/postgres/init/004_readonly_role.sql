-- Local-demo least-privilege role used only by API and dashboard containers.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'forgeflow_reader') THEN
        CREATE ROLE forgeflow_reader LOGIN PASSWORD 'forgeflow_reader_local_only';
    ELSE
        ALTER ROLE forgeflow_reader LOGIN PASSWORD 'forgeflow_reader_local_only';
    END IF;
    EXECUTE format(
        'GRANT CONNECT ON DATABASE %I TO forgeflow_reader',
        current_database()
    );
END
$$;

GRANT USAGE ON SCHEMA observability, quarantine, marts TO forgeflow_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA observability, quarantine, marts TO forgeflow_reader;

ALTER DEFAULT PRIVILEGES FOR ROLE forgeflow IN SCHEMA observability
    GRANT SELECT ON TABLES TO forgeflow_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE forgeflow IN SCHEMA quarantine
    GRANT SELECT ON TABLES TO forgeflow_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE forgeflow IN SCHEMA marts
    GRANT SELECT ON TABLES TO forgeflow_reader;
