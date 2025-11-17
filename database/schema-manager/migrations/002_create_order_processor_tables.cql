-- Migration: Order Processor Tables
-- Version: 2
-- Description: Create tables for order processor service

USE orders_producer_store;

CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY,
    customer_id INT,
    product_id TEXT,
    quantity INT,
    unit_price DECIMAL,
    total DECIMAL,
    timestamp TIMESTAMP,
    status TEXT
);

CREATE TABLE IF NOT EXISTS statistics (
    metric TEXT PRIMARY KEY,
    value COUNTER
);