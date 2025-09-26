-- Upload the tables to postgres
-- CREATE OR REPLACE TABLE postgres_db.TABLE_NAME                                              AS (SELECT * FROM TABLE_NAME)


-- Call Cycles
CREATE OR REPLACE TABLE postgres_db.call_cycles        AS (SELECT * FROM call_cycle_stores);
CREATE OR REPLACE TABLE postgres_db.call_cycle_stores  AS (SELECT * FROM call_cycle_stores);

-- Stores
CREATE OR REPLACE TABLE postgres_db.store_visit_days      AS (SELECT * FROM store_visit_days);
CREATE OR REPLACE TABLE postgres_db.store_additional_reps AS (SELECT * FROM store_additional_reps);
CREATE OR REPLACE TABLE postgres_db.store_contacts        AS (SELECT * FROM store_contacts);
CREATE OR REPLACE TABLE postgres_db.store_notes           AS (SELECT * FROM store_notes);
CREATE OR REPLACE TABLE postgres_db.store_sales_rep_notes AS (SELECT * FROM store_sales_rep_notes);
CREATE OR REPLACE TABLE postgres_db.stores                AS (SELECT * FROM stores);

-- Tasks

CREATE OR REPLACE TABLE postgres_db.task_documents                  AS (SELECT * FROM task_documents);
CREATE OR REPLACE TABLE postgres_db.task_call_cycles                AS (SELECT * FROM task_call_cycles);
CREATE OR REPLACE TABLE postgres_db.task_photos                     AS (SELECT * FROM task_photos);
CREATE OR REPLACE TABLE postgres_db.task_questions                  AS (SELECT * FROM task_questions);
CREATE OR REPLACE TABLE postgres_db.task_rep_images_cannot_complete AS (SELECT * FROM task_rep_images_cannot_complete);
CREATE OR REPLACE TABLE postgres_db.task_comments                   AS (SELECT * FROM task_comments);
CREATE OR REPLACE TABLE postgres_db.task_rep_images                 AS (SELECT * FROM task_rep_images);
CREATE OR REPLACE TABLE postgres_db.tasks                           AS (SELECT * FROM tasks);




