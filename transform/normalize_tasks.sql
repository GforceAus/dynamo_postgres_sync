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

-- Insert into task_photos table
INSERT OR REPLACE INTO
  task_photos
SELECT
  id AS task_uuid,
  unnest (task_photos).task_id,
  unnest (task_photos).client_photos_shareable,
  unnest (task_photos).photo_name,
  unnest (task_photos).task_photos_notes
FROM
  task_raw
WHERE
  task_photos IS NOT NULL
  AND len (task_photos) > 0;

-- Insert into task_questions table
INSERT OR REPLACE INTO
  task_questions
SELECT
  id AS task_uuid,
  unnest (questions).question,
  unnest (questions).client_shareable,
  unnest (questions).Answers,
  unnest (questions).additionShareable,
  unnest (questions).question_shareable,
  unnest (questions).answer_from_rep
FROM
  task_raw
WHERE
  questions IS NOT NULL
  AND len (questions) > 0;

-- Insert into task_rep_images_cannot_complete table
INSERT OR REPLACE INTO
  task_rep_images_cannot_complete
SELECT
  id AS task_uuid,
  unnest (rep_images_cannot_complete).bucket,
  unnest (rep_images_cannot_complete).localUri,
  unnest (rep_images_cannot_complete).mimeType,
  unnest (rep_images_cannot_complete).region,
  unnest (rep_images_cannot_complete).key,
  unnest (rep_images_cannot_complete).isUploaded
FROM
  task_raw
WHERE
  rep_images_cannot_complete IS NOT NULL
  AND len (rep_images_cannot_complete) > 0;
