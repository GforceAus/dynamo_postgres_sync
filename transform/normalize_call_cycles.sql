-- Transform call_cycle_raw data into normalized tables

-- Insert into call_cycle_stores table
INSERT OR REPLACE INTO call_cycle_stores
SELECT
  call_id,
  unnest(stores).store_id,
  unnest(stores).mins_per_visit,
  unnest(stores).anotherDis,
  unnest(stores).supplier_username,
  unnest(stores).store_name,
  unnest(stores).checked,
  unnest(stores).disabled,
  unnest(stores).label,
  unnest(stores).value,
  unnest(stores).frequency,
  unnest(stores).supplier
FROM call_cycle_raw
WHERE stores IS NOT NULL
  AND len(stores) > 0;

-- Insert into main call_cycles table (excluding nested fields)
INSERT OR REPLACE INTO call_cycles
SELECT
  id,
  call_id,
  call_status,
  _lastChangedAt,
  call_time,
  end_date,
  createdAt,
  retailer,
  call_cycle_name,
  -- Extract first country value from array (assuming single value)
  CASE
    WHEN country IS NOT NULL AND len(country) > 0
    THEN unnest(country).value
    ELSE NULL
  END as country,
  -- Extract first state value from array (assuming single value)
  CASE
    WHEN state IS NOT NULL AND len(state) > 0
    THEN unnest(state).value
    ELSE NULL
  END as state,
  _version,
  call_cycle_freq,
  start_date,
  updatedAt,
  supplier_username,
  supplier_name
FROM call_cycle_raw;
