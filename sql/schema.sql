-- Хабы
CREATE TABLE student26.hub_customer (
    customer_hash_key TEXT PRIMARY KEY,
    load_dts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_source TEXT DEFAULT 'SampleSuperstore.csv'
);

CREATE TABLE student26.hub_product (
    product_hash_key TEXT PRIMARY KEY,
    load_dts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_source TEXT DEFAULT 'SampleSuperstore.csv'
);

CREATE TABLE student26.hub_location (
    location_hash_key TEXT PRIMARY KEY,
    load_dts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_source TEXT DEFAULT 'SampleSuperstore.csv'
);

CREATE TABLE student26.hub_order (
    order_hash_key TEXT PRIMARY KEY,
    load_dts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_source TEXT DEFAULT 'SampleSuperstore.csv'
);

-- Линки
CREATE TABLE student26.link_order (
    order_hash_key TEXT PRIMARY KEY,
    customer_hash_key TEXT,
    product_hash_key TEXT,
    location_hash_key TEXT,
    load_dts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_source TEXT DEFAULT 'SampleSuperstore.csv',
    FOREIGN KEY (customer_hash_key) REFERENCES hub_customer(customer_hash_key),
    FOREIGN KEY (product_hash_key) REFERENCES hub_product(product_hash_key),
    FOREIGN KEY (location_hash_key) REFERENCES hub_location(location_hash_key)
);

-- Спутники
CREATE TABLE student26.sat_customer (
    customer_hash_key TEXT,
    load_dts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    segment TEXT,
    region TEXT,
    record_source TEXT DEFAULT 'SampleSuperstore.csv',
    PRIMARY KEY (customer_hash_key, load_dts),
    FOREIGN KEY (customer_hash_key) REFERENCES hub_customer(customer_hash_key)
);

CREATE TABLE student26.sat_product (
    product_hash_key TEXT,
    load_dts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    category TEXT,
    sub_category TEXT,
    record_source TEXT DEFAULT 'SampleSuperstore.csv',
    PRIMARY KEY (product_hash_key, load_dts),
    FOREIGN KEY (product_hash_key) REFERENCES hub_product(product_hash_key)
);

CREATE TABLE student26.sat_location (
    location_hash_key TEXT,
    load_dts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    region TEXT,
    record_source TEXT DEFAULT 'SampleSuperstore.csv',
    PRIMARY KEY (location_hash_key, load_dts),
    FOREIGN KEY (location_hash_key) REFERENCES hub_location(location_hash_key)
);

CREATE TABLE student26.sat_order (
    order_hash_key TEXT,
    load_dts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ship_mode TEXT,
    sales NUMERIC,
    quantity INTEGER,
    discount NUMERIC,
    profit NUMERIC,
    record_source TEXT DEFAULT 'SampleSuperstore.csv',
    PRIMARY KEY (order_hash_key, load_dts),
    FOREIGN KEY (order_hash_key) REFERENCES hub_order(order_hash_key)
);