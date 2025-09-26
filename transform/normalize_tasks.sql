-- Insert into call_cycle_stores table
INSERT OR REPLACE INTO
  task_documents
SELECT
  id AS task_uuid,
  unnest (documentAdd).requiredDoc,
  unnest (documentAdd).document,
  unnest (documentAdd).notes,
  unnest (documentAdd).uploaded,
  unnest (documentAdd).signed,
  unnest (documentAdd).localUri,
  unnest (documentAdd).mimeType,
  unnest (documentAdd).key
FROM
  task_raw
WHERE
  documentAdd IS NOT NULL
  AND len (documentAdd) > 0;
