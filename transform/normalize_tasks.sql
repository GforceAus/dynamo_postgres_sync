-- Insert into task_documents table
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

-- Insert into task_call_cycles table
-- need a tmp table for the to_json cast
CREATE OR REPLACE TABLE tmp AS (
SELECT
  id AS task_uuid,
  unnest (callCycle).call_cycle_name AS call_cycle_name,
  unnest (callCycle).call_status AS call_status,
  unnest (callCycle).retailer AS retailer,
  unnest (callCycle).call_id AS call_id,
  unnest (callCycle).storeList AS storeList,
FROM
  task_raw
WHERE
  callCycle IS NOT NULL
  AND len (callCycle) > 0
);


-- Will automatically cast to JSON
INSERT OR REPLACE INTO
  task_call_cycles
SELECT * FROM tmp;
