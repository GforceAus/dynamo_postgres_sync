CREATE TABLE Store (
    store_id TEXT NOT NULL,
    mins_per_visit TEXT,
    anotherDis BOOLEAN,
    supplier_username TEXT,
    store_name TEXT,
    checked BOOLEAN,
    disabled BOOLEAN,
    label TEXT,
    value TEXT,
    frequency TEXT,
    supplier TEXT
);

CREATE TABLE Country (
    value TEXT,
    label TEXT
);

CREATE TABLE State (
    value TEXT,
    label TEXT
);

CREATE TABLE Item (
    call_status TEXT,
    lastChangedAt DOUBLE,
    call_time TEXT,
    end_date TEXT,
    store_id TEXT, --   <-------------------------------------
    createdAt TEXT,
    retailer TEXT,
    call_cycle_name TEXT,
    country_id TEXT, -- <--------------------------------------
    call_id TEXT,
    state_id TEXT, --   <---------------------------------------
    version DOUBLE,
    call_cycle_freq TEXT,
    start_date TEXT,
    updatedAt TEXT,
    supplier_username TEXT,
    id TEXT,
    supplier_name TEXT
);
