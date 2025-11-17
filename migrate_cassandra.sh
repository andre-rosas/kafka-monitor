#!/bin/bash
# ============================================================================
# Initialize Cassandra Schemas for Kafka Monitor
# ============================================================================

set -e

echo ""
echo "=============================================="
echo "Kafka Monitor - Cassandra Initialization"
echo "=============================================="
echo ""

# Wait for Cassandra to be ready
echo "[1/4] Waiting for Cassandra to accept connections..."
until docker exec -i kafka-monitor-cassandra cqlsh -e "describe keyspaces" > /dev/null 2>&1; do
  echo "Cassandra is not ready yet - waiting..."
  sleep 5
done
echo "Cassandra is ready!"
echo ""

# Create order-processor schema
echo "[2/4] Creating order-processor keyspace and tables..."
if cat order-processor/resources/cassandra/schema.cql | docker exec -i kafka-monitor-cassandra cqlsh; then
  echo "    ✓ order-processor schema created"
else
  echo "    ✗ ERROR creating order-processor schema"
  exit 1
fi
echo ""

# Create query-processor schema
echo "[3/4] Creating query-processor keyspace and tables..."
if cat query-processor/resources/cassandra/schema.cql | docker exec -i kafka-monitor-cassandra cqlsh; then
  echo "    ✓ query-processor schema created"
else
  echo "    ✗ ERROR creating query-processor schema"
  exit 1
fi
echo ""

# Create registry-processor schema
echo "[4/4] Creating registry-processor keyspace and tables..."
if cat registry-processor/resources/cassandra/schema.cql | docker exec -i kafka-monitor-cassandra cqlsh; then
  echo "    ✓ registry-processor schema created"
else
  echo "    ✗ ERROR creating registry-processor schema"
  exit 1
fi
echo ""

echo "============================================"
echo "✓ All Cassandra schemas created successfully!"
echo "============================================"
echo ""
echo "Verifying keyspaces:"
docker exec -i kafka-monitor-cassandra cqlsh -e "DESCRIBE KEYSPACES;"
echo ""