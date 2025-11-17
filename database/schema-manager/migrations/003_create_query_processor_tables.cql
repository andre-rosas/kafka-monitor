-- Migration: Query Processor Tables
-- Version: 3
-- Description: Create tables for query processor service

USE query_processor_store;

CREATE TABLE IF NOT EXISTS orders_by_customer (
    customer_id INT PRIMARY KEY,
    first_order_timestamp TIMESTAMP,
    last_order_id TEXT,
    last_order_timestamp TIMESTAMP,
    total_orders INT,
    total_spent DOUBLE
);

CREATE TABLE IF NOT EXISTS orders_by_product (
    product_id TEXT PRIMARY KEY,
    total_quantity INT,
    total_revenue DOUBLE,
    order_count INT,
    avg_quantity DOUBLE,
    last_order_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders_timeline (
    bucket_id INT,
    timestamp TIMESTAMP,
    order_id TEXT,
    customer_id INT,
    product_id TEXT,
    total DOUBLE,
    status TEXT,
    PRIMARY KEY (bucket_id, timestamp, order_id)
) WITH CLUSTERING ORDER BY (timestamp DESC);

CREATE TABLE IF NOT EXISTS processing_stats (
    processor_id TEXT PRIMARY KEY,
    processed_count BIGINT,
    error_count BIGINT,
    last_processed_timestamp TIMESTAMP
);

CREATE INDEX IF NOT EXISTS orders_timeline_order_id_idx ON orders_timeline (order_id);