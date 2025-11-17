#!/bin/bash
set -e

echo "=== Kafka Monitor - Cassandra Migrations ==="
echo ""

CASSANDRA_HOST="${CASSANDRA_HOST:-cassandra}"
CASSANDRA_PORT="${CASSANDRA_PORT:-9042}"
MAX_RETRIES=60
RETRY_INTERVAL=5

echo "[1/3] Waiting for Cassandra at ${CASSANDRA_HOST}:${CASSANDRA_PORT}..."

# Wait for Cassandra to be ready with timeout
attempt=0
while [ $attempt -lt $MAX_RETRIES ]; do
    if cqlsh ${CASSANDRA_HOST} ${CASSANDRA_PORT} -e 'DESCRIBE KEYSPACES' > /dev/null 2>&1; then
        echo "✓ Cassandra is ready!"
        break
    fi
    attempt=$((attempt + 1))
    echo "Attempt ${attempt}/${MAX_RETRIES} - waiting..."
    sleep ${RETRY_INTERVAL}
done

if [ $attempt -eq $MAX_RETRIES ]; then
    echo "✗ ERROR: Cassandra did not become ready after $((MAX_RETRIES * RETRY_INTERVAL))s"
    exit 1
fi

echo ""
echo "[2/3] Executing migrations..."

# Execute migrations in numerical order
migration_count=0
for migration in /migrations/*.cql; do
    if [ ! -f "$migration" ]; then
        echo "✗ ERROR: No .cql file found in /migrations/"
        exit 1
    fi
    
    migration_name=$(basename "$migration")
    echo "  → Executing: ${migration_name}"
    
    if cqlsh ${CASSANDRA_HOST} ${CASSANDRA_PORT} -f "$migration"; then
        echo "    ✓ Success"
        migration_count=$((migration_count + 1))
    else
        echo "    ✗ ERROR executing ${migration_name}"
        exit 1
    fi
done

echo ""
echo "[3/3] Validating schemas..."
echo ""
echo "Created Keyspaces:"
cqlsh ${CASSANDRA_HOST} ${CASSANDRA_PORT} -e "DESCRIBE KEYSPACES;"
echo ""

echo "============================================"
echo "✓ ${migration_count} migrations executed successfully!"
echo "============================================"