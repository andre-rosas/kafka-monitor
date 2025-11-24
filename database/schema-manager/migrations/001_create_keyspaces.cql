-- Migration: Create Keyspaces
-- Version: 1
-- Description: Create all necessary keyspaces for the application

CREATE KEYSPACE IF NOT EXISTS order_processor
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE KEYSPACE IF NOT EXISTS query_processor
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE KEYSPACE IF NOT EXISTS registry_processor
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};