CREATE OR REPLACE TABLE call_cycles (
  id VARCHAR,
  -- equally as unique as id and descriptive
  call_id VARCHAR PRIMARY KEY,
  call_status VARCHAR,
  _lastChangedAt DOUBLE,
  call_time VARCHAR,
  end_date VARCHAR,
  createdAt VARCHAR,
  retailer VARCHAR,
  call_cycle_name VARCHAR,
  -- Just use the value of the country, it's sufficiently obvious
  country VARCHAR,
  -- Just use the value of the state, it's sufficiently obvious
  state VARCHAR,
  _version DOUBLE,
  call_cycle_freq VARCHAR,
  start_date VARCHAR,
  updatedAt VARCHAR,
  supplier_username VARCHAR,
  supplier_name VARCHAR
);

CREATE OR REPLACE TABLE call_cycle_stores (
  call_id VARCHAR,
  store_id VARCHAR,
  mins_per_visit VARCHAR,
  anotherDis BOOLEAN,
  supplier_username VARCHAR,
  store_name VARCHAR,
  checked BOOLEAN,
  disabled BOOLEAN,
  "label" VARCHAR,
  "value" VARCHAR,
  frequency VARCHAR,
  supplier VARCHAR,
  PRIMARY KEY (call_id, store_id)
);
