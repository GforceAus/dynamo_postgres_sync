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

-- Insert into task_rep_images table
INSERT OR REPLACE INTO
  task_rep_images
SELECT
  id AS task_uuid,
  task_id,
  store_id,
  store_name,
  supplier_id,
  supplier_name,
  state,
  unnest (rep_images).bucket,
  unnest (rep_images).localUri,
  unnest (rep_images).mimeType,
  unnest (rep_images).region,
  unnest (rep_images).key,
  unnest (rep_images).isUploaded,
  -- Array is in the same order
  -- Sometimes this is null, no reason seemingly
  -- No impact on photo availablility
  -- If it's nullable, there's no point, derive it from key
  -- unnest (photos_from_rep) AS filename
FROM
  task_raw r,
WHERE
  r.rep_images IS NOT NULL
  AND len (r.rep_images) > 0;

-- Insert into task_comments table
INSERT OR REPLACE INTO
  task_comments
SELECT
  id AS task_uuid,
  unnest (task_comments).task_id,
  unnest (task_comments).comment,
  unnest (task_comments).task_comments_notes,
  unnest (task_comments).client_comments_shareable
FROM
  task_raw
WHERE
  task_comments IS NOT NULL
  AND len (task_comments) > 0;



-- Need a tmp table for JSON casting
CREATE OR REPLACE TABLE tmp AS (
SELECT
  id,
  cover_rep_first_name,
  support_rep_last_name,
  endDate,
  retailer_name,
  cannot_complete_reason,
  -- Just use the value field
  country,
  -- Just use the value field
  state,
  logo_img,
  cover_rep_last_name,
  startDate,
  SK,
  supplier_name,
  taskDate,
  _lastChangedAt,
  pause_task_reason,
  store_id,
  time_spent,
  task_name,
  comments_from_rep,
  support_rep_first_name,
  task_description,
  delegated,
  week_number,
  cover_rep_type,
  task_id,
  senior_rep_first_name,
  recurring,
  full_company_name,
  PK,
  store_name,
  support_rep_username,
  task_type,
  created_date,
  week_startDate,
  supplier_id,
  stores.state AS store_state,
  _version,
  task_priority,
  feedback_reassign,
  task_approval,
  taskDateISO8601,
  cannot_complete_comments,
  senior_rep_username,
  record_time,
  fine_line,
  oneOff,
  task_approval_notes,
  visit_freq,
  cover_rep_username,
  task_status,
  updatedAt,
  recurringValue,
  senior_rep_last_name,
  push_task_comments,
  solved,
  delegated_to_sup_rep,
  delegated_comments,
FROM
  task_raw);

INSERT OR REPLACE INTO
  tasks
SELECT * FROM tmp
