CREATE OR REPLACE TABLE task_documents (
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

CREATE OR REPLACE TABLE task_call_cycles (
    task_uuid VARCHAR, --task_raw.id (disambiguate from task_id)
    call_cycle_name VARCHAR,
    call_status VARCHAR,
    retailer VARCHAR,
    call_id VARCHAR,
    PRIMARY KEY (task_uuid, call_id)
);

CREATE OR REPLACE TABLE task_call_cycles_stores (
    task_uuid VARCHAR, --task_raw.id (disambiguate from task_id)
    call_id VARCHAR,
    store_id VARCHAR,
    store_name VARCHAR,
    PRIMARY KEY (task_uuid, call_id, store_id)
);

CREATE OR REPLACE TABLE task_photos (
    task_uuid VARCHAR, --task_raw.id (disambiguate from task_id)
    task_id VARCHAR,
    client_photos_shareable VARCHAR,
    photo_name VARCHAR,
    task_photos_notes VARCHAR,
    -- Only task_UUID is a primary key as the task_photos field is ALWAYS exactly 1
    PRIMARY KEY (task_uuid),
);


CREATE TABLE tasks (
  id VARCHAR PRIMARY KEY,
  cover_rep_first_name VARCHAR,
  support_rep_last_name VARCHAR,
  endDate VARCHAR,
  retailer_name VARCHAR,
  cannot_complete_reason VARCHAR,
  -- Just use the value field
  country VARCHAR,
  -- Just use the value field
  state VARCHAR,
  logo_img VARCHAR,
  cover_rep_last_name VARCHAR,
  startDate VARCHAR,
  SK VARCHAR,
  supplier_name VARCHAR,
  taskDate VARCHAR,
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
  created_date VARCHAR,
  week_startDate VARCHAR,
  supplier_id VARCHAR,
  -- Checked is always true, so we always include the store_id and store_name,
  store_id VARCHAR,
  store_name VARCHAR,
  store_state VARCHAR,
  _version DOUBLE,
  task_priority VARCHAR,
  feedback_reassign VARCHAR,
  task_approval VARCHAR,
  taskDateISO8601 DATE,
  cannot_complete_comments VARCHAR,
  senior_rep_username VARCHAR,
  "action" JSON,
  record_time VARCHAR,
  fine_line VARCHAR,
  oneOff BOOLEAN,
  task_approval_notes VARCHAR,
  visit_freq VARCHAR,
  cover_rep_username VARCHAR,
  task_status VARCHAR,
  updatedAt VARCHAR,
  recurringValue VARCHAR,
  senior_rep_last_name VARCHAR,
  push_task_comments VARCHAR,
  solved BOOLEAN,
  delegated_to_sup_rep VARCHAR,
  delegated_comments VARCHAR
);


CREATE OR REPLACE TABLE task_questions (
    task_uuid VARCHAR, --task_raw.id (disambiguate from task_id)
    question VARCHAR,
    client_shareable VARCHAR,
    Answers VARCHAR [],
    additionShareable VARCHAR,
    question_shareable BOOLEAN,
    answer_from_rep VARCHAR,
    PRIMARY KEY (task_uuid, question)
);


CREATE OR REPLACE TABLE task_rep_images_cannot_complete (
    task_uuid VARCHAR, --task_raw.id (disambiguate from task_id)
    bucket VARCHAR,
    localUri VARCHAR,
    mimeType VARCHAR,
    region VARCHAR,
    "key" VARCHAR,
    isUploaded VARCHAR,
    PRIMARY KEY (task_uuid, key)
);

CREATE OR REPLACE TABLE task_rep_images (
    task_uuid VARCHAR, --task_raw.id (disambiguate from task_id)
    task_id VARCHAR,
    store_id VARCHAR,
    store_name VARCHAR,
    supplier_id VARCHAR,
    supplier_name VARCHAR,
    state VARCHAR,
    bucket VARCHAR,
    localUri VARCHAR,
    mimeType VARCHAR,
    region VARCHAR,
    "key" VARCHAR,
    isUploaded VARCHAR,
    -- task_raw.photos_from_rep
    filename VARCHAR,
    PRIMARY KEY (task_uuid, key)
);

CREATE OR REPLACE TABLE task_comments (
    task_uuid VARCHAR, --task_raw.id (disambiguate from task_id)
    task_id VARCHAR,
    "comment" VARCHAR,
    task_comments_notes VARCHAR,
    client_comments_shareable VARCHAR,
    PRIMARY KEY (task_uuid, comment)
)
