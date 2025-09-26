CREATE
OR REPLACE TABLE stores (
  id VARCHAR PRIMARY KEY,
  store_manager VARCHAR,
  support_rep_username VARCHAR,
  retailer_name VARCHAR,
  trade_drive_trough VARCHAR,
  visit_time VARCHAR,
  country VARCHAR,
  state VARCHAR,
  version DOUBLE,
  territory VARCHAR,
  whare_sfs VARCHAR,
  region VARCHAR,
  support_status VARCHAR,
  senior_rep_username VARCHAR,
  lastChangedAt DOUBLE,
  store_grade VARCHAR,
  store_id VARCHAR,
  senior_status VARCHAR,
  createdAt VARCHAR,
  address VARCHAR,
  visit_freq VARCHAR,
  updatedAt VARCHAR,
  store_size VARCHAR,
  mob_number VARCHAR,
  store_manager_email VARCHAR,
  visit_start_week_no VARCHAR,
  store_name VARCHAR,
  store_status VARCHAR,
  sales_status VARCHAR,
  sales_rep_username VARCHAR
);

CREATE
OR REPLACE TABLE store_visit_days (
  store_id VARCHAR,
  name VARCHAR,
  checked BOOLEAN,
  PRIMARY KEY (store_id, name)
);

CREATE
OR REPLACE TABLE store_additional_reps (
  store_id VARCHAR,
  rep_cover VARCHAR,
  last_name VARCHAR,
  from_date VARCHAR,
  to_date VARCHAR,
  rep_cover_username VARCHAR,
  first_name VARCHAR,
  PRIMARY KEY (store_id, rep_cover_username)
);

CREATE
OR REPLACE TABLE store_contacts (
  store_id VARCHAR,
  "name" VARCHAR,
  "position" VARCHAR,
  phone VARCHAR,
  email VARCHAR,
  PRIMARY KEY (store_id, email)
);

CREATE
OR REPLACE TABLE store_notes (
  store_id VARCHAR,
  datetime VARCHAR,
  notes VARCHAR,
  PRIMARY KEY (store_id, datetime)
);

CREATE
OR REPLACE TABLE store_sales_rep_notes (
  store_id VARCHAR,
  datetime VARCHAR,
  notes VARCHAR,
  PRIMARY KEY (store_id, datetime)
);
