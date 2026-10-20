-- Store the signature headers sent with each attempt, so the dashboard can show them.
ALTER TABLE delivery_attempts ADD COLUMN request_headers jsonb;
