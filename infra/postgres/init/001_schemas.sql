-- ForgeFlow's warehouse layers are explicit so ownership and lineage remain visible.
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS observability;
CREATE SCHEMA IF NOT EXISTS quarantine;

COMMENT ON SCHEMA raw IS 'Typed, accepted source records with ingestion lineage.';
COMMENT ON SCHEMA staging IS 'dbt-cleaned source-conformed views.';
COMMENT ON SCHEMA intermediate IS 'Reusable dbt transformations and incremental state.';
COMMENT ON SCHEMA marts IS 'Reviewer-facing analytical facts, dimensions, and aggregates.';
COMMENT ON SCHEMA observability IS 'Pipeline, file, quality, artifact, lineage, and incident evidence.';
COMMENT ON SCHEMA quarantine IS 'Rejected source records retained with structured reasons.';
