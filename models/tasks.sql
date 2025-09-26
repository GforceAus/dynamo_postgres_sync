



-- TODO Cast dates
-- D select CAST(taskDateISO8601 AS DATE), CAST(created_date AS DATETIME), CAST(updatedAt AS DATETIME) from task_raw;
CREATE
OR REPLACE TABLE task_documents (
  task_uuid VARCHAR, --task_raw.id (disambiguate from task_id)
  requiredDoc BOOLEAN,
  "document" VARCHAR,
  notes VARCHAR,
  uploaded BOOLEAN,
  signed BOOLEAN,
  localUri VARCHAR,
  mimeType VARCHAR,
  "key" VARCHAR,
  PRIMARY KEY (task_uuid, document)
);

CREATE
OR REPLACE TABLE task_call_cycles (
  task_uuid VARCHAR, --task_raw.id (disambiguate from task_id)
  call_cycle_name VARCHAR,
  call_status VARCHAR,
  retailer VARCHAR,
  call_id VARCHAR,
  storeList JSON,
  PRIMARY KEY (task_uuid, call_id)
);

-- This would give 20 million rows
-- SELECT id, call_id, unnest(storeList).store_id, unnest(storeList).store_name FROM (select unnest(callCycle, recursive := true), id from task_raw);
CREATE
OR REPLACE TABLE task_photos (
  task_uuid VARCHAR, --task_raw.id (disambiguate from task_id)
  task_id VARCHAR,
  client_photos_shareable VARCHAR,
  photo_name VARCHAR,
  task_photos_notes VARCHAR,
  -- Only task_UUID is a primary key as the task_photos field is ALWAYS exactly 1
  PRIMARY KEY (task_uuid, photo_name),
);

CREATE
OR REPLACE TABLE tasks (
  id VARCHAR PRIMARY KEY,
  -- Use the taskDateISO8601 value
  taskDate DATE,
  updatedAt DATETIME,
  startDate DATETIME,
  week_startDate DATE,
  endDate DATETIME,
  created_date DATETIME,
  cover_rep_first_name VARCHAR,
  support_rep_last_name VARCHAR,
  retailer_name VARCHAR,
  cannot_complete_reason VARCHAR,
  -- Just use the value field
  task_countries JSON,
  -- Just use the value field
  task_states JSON,
  logo_img VARCHAR,
  cover_rep_last_name VARCHAR,
  SK VARCHAR,
  supplier_name VARCHAR,
  _lastChangedAt DOUBLE,
  pause_task_reason VARCHAR,
  store_id VARCHAR,
  time_spent VARCHAR,
  task_name VARCHAR,
  comments_from_rep VARCHAR,
  support_rep_first_name VARCHAR,
  task_description VARCHAR,
  delegated BOOLEAN,
  week_number VARCHAR,
  cover_rep_type VARCHAR,
  task_id VARCHAR,
  senior_rep_first_name VARCHAR,
  recurring BOOLEAN,
  full_company_name VARCHAR,
  PK VARCHAR,
  store_name VARCHAR,
  support_rep_username VARCHAR,
  task_type VARCHAR,
  supplier_id VARCHAR,
  store_state VARCHAR,
  _version DOUBLE,
  task_priority VARCHAR,
  feedback_reassign VARCHAR,
  task_approval VARCHAR,
  cannot_complete_comments VARCHAR,
  senior_rep_username VARCHAR,
  record_time VARCHAR,
  fine_line VARCHAR,
  oneOff BOOLEAN,
  task_approval_notes VARCHAR,
  visit_freq VARCHAR,
  cover_rep_username VARCHAR,
  task_status VARCHAR,
  recurringValue VARCHAR,
  senior_rep_last_name VARCHAR,
  push_task_comments VARCHAR,
  solved BOOLEAN,
  delegated_to_sup_rep VARCHAR,
  delegated_comments VARCHAR
);

CREATE
OR REPLACE TABLE task_questions (
  task_uuid VARCHAR, --task_raw.id (disambiguate from task_id)
  question VARCHAR,
  client_shareable VARCHAR,
  Answers VARCHAR [],
  additionShareable VARCHAR,
  question_shareable BOOLEAN,
  answer_from_rep VARCHAR,
  PRIMARY KEY (task_uuid, question)
);

CREATE
OR REPLACE TABLE task_rep_images_cannot_complete (
  task_uuid VARCHAR, --task_raw.id (disambiguate from task_id)
  bucket VARCHAR,
  localUri VARCHAR,
  mimeType VARCHAR,
  region VARCHAR,
  "key" VARCHAR,
  isUploaded VARCHAR,
  PRIMARY KEY (task_uuid, key)
);

CREATE
OR REPLACE TABLE task_rep_images (
  task_uuid VARCHAR, --task_raw.id (disambiguate from task_id)
  task_id VARCHAR,
  store_id VARCHAR,
  store_name VARCHAR,
  supplier_id VARCHAR,
  supplier_name VARCHAR,
  state VARCHAR,
  task_date DATE,
  bucket VARCHAR,
  localUri VARCHAR,
  mimeType VARCHAR,
  region VARCHAR,
  "key" VARCHAR,
  isUploaded VARCHAR,
  photo_datetime DATETIME,
  -- task_raw.photos_from_rep
  -- filename VARCHAR,
  PRIMARY KEY (task_uuid, key)
);

CREATE
OR REPLACE TABLE task_comments (
  task_uuid VARCHAR, --task_raw.id (disambiguate from task_id)
  task_id VARCHAR,
  "comment" VARCHAR,
  task_comments_notes VARCHAR,
  client_comments_shareable VARCHAR,
  PRIMARY KEY (task_uuid, comment)
);
