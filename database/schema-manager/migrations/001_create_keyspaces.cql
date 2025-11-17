-- Migration: Create Keyspaces
-- Version: 1
-- Description: Create all necessary keyspaces for the application

CREATE KEYSPACE IF NOT EXISTS orders_producer_store 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE KEYSPACE IF NOT EXISTS query_processor_store 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE KEYSPACE IF NOT EXISTS registry_processor_store 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};