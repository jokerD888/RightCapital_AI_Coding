-- Apply once to an existing Step 1/2 database before starting this release.
-- Safe to rerun. Fresh installations already have this column in schema.sql.
BEGIN;
ALTER TABLE notification_tasks ADD COLUMN IF NOT EXISTS retry_start_attempt integer NOT NULL
    DEFAULT 0 CHECK (retry_start_attempt >= 0 AND retry_start_attempt <= attempt_count);
COMMIT;
