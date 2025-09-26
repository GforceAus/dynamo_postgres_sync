-- Upload the tables to postgres
-- CREATE OR REPLACE TABLE postgres_db.TABLE_NAME AS (SELECT * FROM TABLE_NAME)

----------------------------------------
-- Replacements ------------------------
----------------------------------------

-- Call Cycles
CREATE OR REPLACE TABLE postgres_db.call_cycles        AS SELECT * FROM call_cycles;
CREATE OR REPLACE TABLE postgres_db.call_cycle_stores  AS (SELECT * FROM call_cycle_stores);

CALL postgres_execute('postgres_db', 'ALTER TABLE call_cycles ADD PRIMARY KEY (call_id)');
CALL postgres_execute('postgres_db', 'ALTER TABLE call_cycle_stores ADD PRIMARY KEY (call_id, store_id)');


-- Stores
CREATE OR REPLACE TABLE postgres_db.store_visit_days      AS (SELECT * FROM store_visit_days);
CREATE OR REPLACE TABLE postgres_db.store_additional_reps AS (SELECT * FROM store_additional_reps);
CREATE OR REPLACE TABLE postgres_db.store_contacts        AS (SELECT * FROM store_contacts);
CREATE OR REPLACE TABLE postgres_db.store_notes           AS (SELECT * FROM store_notes);
CREATE OR REPLACE TABLE postgres_db.store_sales_rep_notes AS (SELECT * FROM store_sales_rep_notes);
CREATE OR REPLACE TABLE postgres_db.stores                AS (SELECT * FROM stores);

CALL postgres_execute('postgres_db', 'ALTER TABLE store_visit_days ADD PRIMARY KEY (store_id, name)');
CALL postgres_execute('postgres_db', 'ALTER TABLE store_additional_reps ADD PRIMARY KEY (store_id, rep_cover_username)');
CALL postgres_execute('postgres_db', 'ALTER TABLE store_contacts ADD PRIMARY KEY (store_id, email)');
CALL postgres_execute('postgres_db', 'ALTER TABLE store_notes ADD PRIMARY KEY (store_id, datetime)');
CALL postgres_execute('postgres_db', 'ALTER TABLE store_sales_rep_notes ADD PRIMARY KEY (store_id, datetime)');
CALL postgres_execute('postgres_db', 'ALTER TABLE stores ADD PRIMARY KEY (id)');

-- Tasks
CREATE OR REPLACE TABLE postgres_db.task_documents                  AS (SELECT * FROM task_documents);
CREATE OR REPLACE TABLE postgres_db.task_call_cycles                AS (SELECT * FROM task_call_cycles);
CREATE OR REPLACE TABLE postgres_db.task_photos                     AS (SELECT * FROM task_photos);
CREATE OR REPLACE TABLE postgres_db.task_rep_images_cannot_complete AS (SELECT * FROM task_rep_images_cannot_complete);
CREATE OR REPLACE TABLE postgres_db.task_comments                   AS (SELECT * FROM task_comments);

CALL postgres_execute('postgres_db', 'ALTER TABLE task_documents ADD PRIMARY KEY (task_uuid, document)');
CALL postgres_execute('postgres_db', 'ALTER TABLE task_call_cycles ADD PRIMARY KEY (task_uuid, call_id)');
CALL postgres_execute('postgres_db', 'ALTER TABLE task_photos ADD PRIMARY KEY (task_uuid, photo_name)');
CALL postgres_execute('postgres_db', 'ALTER TABLE task_rep_images_cannot_complete ADD PRIMARY KEY (task_uuid, key)');
CALL postgres_execute('postgres_db', 'ALTER TABLE task_comments ADD PRIMARY KEY (task_uuid, comment)');
CALL pg_clear_cache();

-- These are appended for archival purposes (just in case they're dropped upstream)
CREATE OR REPLACE TABLE postgres_db.task_questions                  AS (SELECT * FROM task_questions);
CREATE OR REPLACE TABLE postgres_db.task_rep_images                 AS (SELECT * FROM task_rep_images);
CREATE OR REPLACE TABLE postgres_db.tasks                           AS (SELECT * FROM tasks);

CALL postgres_execute('postgres_db', 'ALTER TABLE tasks ADD PRIMARY KEY (id)');
CALL postgres_execute('postgres_db', 'ALTER TABLE task_rep_images ADD PRIMARY KEY (task_uuid, key)');
CALL postgres_execute('postgres_db', 'ALTER TABLE task_questions ADD PRIMARY KEY (task_uuid, question)');

-- Create indexes
CALL postgres_execute('postgres_db', 'CREATE INDEX idx_task_rep_images_photo_datetime ON task_rep_images (photo_datetime)');
CALL postgres_execute('postgres_db', 'CREATE INDEX idx_tasks_supplier_id ON tasks (supplier_id)');
