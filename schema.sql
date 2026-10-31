BEGIN;

-- One mutable row per vendor. Every delivery attempt reads this current row.
CREATE TABLE IF NOT EXISTS vendors (
    code varchar(64) PRIMARY KEY,
    name text NOT NULL,
    endpoint_url text NOT NULL,
    http_method text NOT NULL DEFAULT 'POST'
        CHECK (http_method IN ('POST', 'PUT', 'PATCH')),
    -- Both templates are Go text/template strings. The json helper escapes values.
    headers_template text NOT NULL DEFAULT '{}',
    body_template text NOT NULL,
    timeout_ms integer NOT NULL DEFAULT 10000 CHECK (timeout_ms BETWEEN 100 AND 60000),
    max_attempts integer NOT NULL DEFAULT 8 CHECK (max_attempts BETWEEN 1 AND 100),
    retry_base_seconds integer NOT NULL DEFAULT 5 CHECK (retry_base_seconds > 0),
    retry_max_seconds integer NOT NULL DEFAULT 1800
        CHECK (retry_max_seconds >= retry_base_seconds)
);

CREATE TABLE IF NOT EXISTS notification_tasks (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system varchar(64) NOT NULL,
    idempotency_key varchar(128) NOT NULL,
    request_hash char(64) NOT NULL,
    vendor_code varchar(64) NOT NULL REFERENCES vendors(code),
    event_type varchar(128) NOT NULL,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    status text NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'PROCESSING', 'RETRY_WAIT', 'SUCCEEDED', 'DEAD')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    retry_start_attempt integer NOT NULL DEFAULT 0
        CHECK (retry_start_attempt >= 0 AND retry_start_attempt <= attempt_count),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    lease_token uuid,
    lease_until timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    UNIQUE (source_system, idempotency_key),
    CHECK (
        (status = 'PROCESSING' AND lease_token IS NOT NULL AND lease_until IS NOT NULL)
        OR (status <> 'PROCESSING' AND lease_token IS NULL AND lease_until IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS notification_tasks_due_idx
    ON notification_tasks (next_attempt_at, id)
    WHERE status IN ('PENDING', 'RETRY_WAIT');
CREATE INDEX IF NOT EXISTS notification_tasks_lease_idx
    ON notification_tasks (lease_until)
    WHERE status = 'PROCESSING';

CREATE TABLE IF NOT EXISTS delivery_logs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_id uuid NOT NULL REFERENCES notification_tasks(id),
    attempt_no integer NOT NULL CHECK (attempt_no > 0),
    -- Includes endpoint, method, templates, timeout and retry policy for this attempt.
    vendor_snapshot jsonb NOT NULL CHECK (jsonb_typeof(vendor_snapshot) = 'object'),
    lease_token uuid NOT NULL,
    outcome text NOT NULL DEFAULT 'STARTED'
        CHECK (outcome IN ('STARTED', 'SUCCESS', 'RETRYABLE_FAILURE',
                           'PERMANENT_FAILURE', 'UNKNOWN')),
    http_status integer CHECK (http_status BETWEEN 100 AND 599),
    response_excerpt text,
    error_message text,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    UNIQUE (task_id, attempt_no)
);

-- Local test endpoints only. Run a mock vendor on port 9090 or update these URLs.
-- Rerunning the schema preserves existing vendor edits.
INSERT INTO vendors
    (code, name, endpoint_url, http_method, headers_template, body_template)
VALUES
    ('ads', 'Advertising conversion', 'http://127.0.0.1:9090/ads/conversions', 'POST',
     $tpl${"Content-Type":"application/json","X-Event-Type":{{json .EventType}},"Idempotency-Key":{{json .TaskID}}}$tpl$,
     $tpl${"event":{{json .EventType}},"external_user_id":{{json .Payload.user_id}},"click_id":{{json .Payload.click_id}}}$tpl$),
    ('crm', 'CRM contact update', 'http://127.0.0.1:9090/crm/contacts', 'PATCH',
     $tpl${"Content-Type":"application/json","X-Source-System":{{json .SourceSystem}},"Idempotency-Key":{{json .TaskID}}}$tpl$,
     $tpl${"contact_id":{{json .Payload.contact_id}},"fields":{"status":{{json .Payload.status}}}}$tpl$),
    ('inventory', 'Inventory adjustment', 'http://127.0.0.1:9090/inventory/adjustments', 'POST',
     $tpl${"Content-Type":"application/json","X-Event-Type":{{json .EventType}},"Idempotency-Key":{{json .TaskID}}}$tpl$,
     $tpl${"sku":{{json .Payload.sku}},"quantity_delta":{{json .Payload.delta}},"reference":{{json .Payload.order_id}}}$tpl$)
ON CONFLICT (code) DO NOTHING;

COMMIT;
