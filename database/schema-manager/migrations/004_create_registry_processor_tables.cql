-- Migration: Registry Processor Tables
-- Version: 4
-- Description: Create tables for registry processor service

USE registry_processor_store;

CREATE TABLE IF NOT EXISTS registered_orders (
    order_id TEXT PRIMARY KEY,
    customer_id INT,
    product_id TEXT,
    quantity INT,
    total DOUBLE,
    status TEXT,
    registered_at TIMESTAMP,
    updated_at TIMESTAMP,
    version INT,
    validation_passed BOOLEAN
);

CREATE TABLE IF NOT EXISTS order_updates (
    order_id TEXT,
    version INT,
    previous_status TEXT,
    new_status TEXT,
    updated_at TIMESTAMP,
    update_reason TEXT,
    PRIMARY KEY (order_id, version)
) WITH CLUSTERING ORDER BY (version DESC);

CREATE TABLE IF NOT EXISTS validation_stats (
    processor_id TEXT PRIMARY KEY,
    total_validated BIGINT,
    total_approved BIGINT,
    total_rejected BIGINT,
    timestamp TIMESTAMP
);