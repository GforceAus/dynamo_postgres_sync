-- Transform stores_raw data into normalized tables

-- Insert into store_visit_days table
INSERT OR REPLACE INTO store_visit_days (store_id, name, checked)
SELECT
  id as store_id,
  unnest(visit_days).name,
  unnest(visit_days).checked
FROM stores_raw
WHERE visit_days IS NOT NULL
  AND len(visit_days) > 0;

-- Insert into store_additional_reps table
INSERT OR REPLACE INTO store_additional_reps
SELECT
  id as store_id,
  unnest(additionalRep).rep_cover,
  unnest(additionalRep).last_name,
  unnest(additionalRep).from_date,
  unnest(additionalRep).to_date,
  unnest(additionalRep).rep_cover_username,
  unnest(additionalRep).first_name
FROM stores_raw
WHERE additionalRep IS NOT NULL
  AND len(additionalRep) > 0;

-- Insert into store_contacts table
INSERT OR REPLACE INTO store_contacts
SELECT
  id as store_id,
  unnest(contact).name,
  unnest(contact).position,
  unnest(contact).phone,
  unnest(contact).email
FROM stores_raw
WHERE contact IS NOT NULL
  AND len(contact) > 0;

-- Insert into store_notes table
INSERT OR REPLACE INTO store_notes
SELECT
  id as store_id,
  unnest(notes).datetime,
  unnest(notes).notes
FROM stores_raw
WHERE notes IS NOT NULL
  AND len(notes) > 0;

-- Insert into store_sales_rep_notes table
INSERT OR REPLACE INTO store_sales_rep_notes
SELECT
  id as store_id,
  unnest(sales_rep_notes).datetime,
  unnest(sales_rep_notes).notes
FROM stores_raw
WHERE sales_rep_notes IS NOT NULL
  AND len(sales_rep_notes) > 0;

-- Insert into main stores table (excluding nested fields)
INSERT OR REPLACE INTO stores
SELECT
  id,
  store_manager,
  support_rep_username,
  retailer_name,
  trade_drive_trough,
  visit_time,
  country,
  state,
  _version,
  territory,
  whare_sfs,
  region,
  support_status,
  senior_rep_username,
  _lastChangedAt,
  store_grade,
  store_id,
  senior_status,
  createdAt,
  address,
  visit_freq,
  updatedAt,
  store_size,
  mob_number,
  store_manager_email,
  visit_start_week_no,
  store_name,
  store_status,
  sales_status,
  sales_rep_username
FROM stores_raw;
