#!/bin/bash

# Kafka Monitor - Kafka Management Script
# Provides commands for managing Kafka topics, consumers, and streaming

set -e

KAFKA_CONTAINER="kafka-monitor-kafka"
KAFKA_BROKER="localhost:9092"
KAFKA_INTERNAL="kafka:9093"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Helper functions
print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_info() {
    echo -e "${YELLOW}ℹ${NC} $1"
}

# Check if Kafka is running
check_kafka() {
    if ! docker ps | grep -q $KAFKA_CONTAINER; then
        print_error "Kafka container is not running!"
        echo "Run: docker-compose up -d kafka"
        exit 1
    fi
    print_success "Kafka is running"
}

# =============================================================================
# Topic Management
# =============================================================================

create_topics() {
    print_info "Creating Kafka topics..."
    
    # Orders topic (main input)
    docker exec $KAFKA_CONTAINER kafka-topics \
        --bootstrap-server $KAFKA_INTERNAL \
        --create \
        --topic orders \
        --partitions 3 \
        --replication-factor 1 \
        --if-not-exists
    print_success "Created topic: orders (3 partitions)"
    
    # Registry topic (approved orders)
    docker exec $KAFKA_CONTAINER kafka-topics \
        --bootstrap-server $KAFKA_INTERNAL \
        --create \
        --topic registry \
        --partitions 3 \
        --replication-factor 1 \
        --if-not-exists
    print_success "Created topic: registry (3 partitions)"
}

list_topics() {
    print_info "Listing all topics..."
    docker exec $KAFKA_CONTAINER kafka-topics \
        --bootstrap-server $KAFKA_INTERNAL \
        --list
}

describe_topic() {
    local topic=$1
    if [ -z "$topic" ]; then
        print_error "Usage: ./kafka.sh describe-topic <topic-name>"
        exit 1
    fi
    
    print_info "Describing topic: $topic"
    docker exec $KAFKA_CONTAINER kafka-topics \
        --bootstrap-server $KAFKA_INTERNAL \
        --describe \
        --topic $topic
}

delete_topic() {
    local topic=$1
    if [ -z "$topic" ]; then
        print_error "Usage: ./kafka.sh delete-topic <topic-name>"
        exit 1
    fi
    
    print_info "Deleting topic: $topic"
    docker exec $KAFKA_CONTAINER kafka-topics \
        --bootstrap-server $KAFKA_INTERNAL \
        --delete \
        --topic $topic
    print_success "Deleted topic: $topic"
}

# =============================================================================
# Consumer Groups
# =============================================================================

list_consumer_groups() {
    print_info "Listing consumer groups..."
    docker exec $KAFKA_CONTAINER kafka-consumer-groups \
        --bootstrap-server $KAFKA_INTERNAL \
        --list
}

describe_consumer_group() {
    local group=$1
    if [ -z "$group" ]; then
        print_error "Usage: ./kafka.sh describe-group <group-name>"
        print_info "Available groups:"
        list_consumer_groups
        exit 1
    fi
    
    print_info "Describing consumer group: $group"
    docker exec $KAFKA_CONTAINER kafka-consumer-groups \
        --bootstrap-server $KAFKA_INTERNAL \
        --describe \
        --group $group
}

reset_consumer_group() {
    local group=$1
    if [ -z "$group" ]; then
        print_error "Usage: ./kafka.sh reset-group <group-name>"
        exit 1
    fi
    
    print_info "Resetting consumer group: $group"
    docker exec $KAFKA_CONTAINER kafka-consumer-groups \
        --bootstrap-server $KAFKA_INTERNAL \
        --reset-offsets \
        --to-earliest \
        --execute \
        --group $group \
        --all-topics
    print_success "Reset consumer group: $group"
}

# =============================================================================
# Message Operations
# =============================================================================

consume_messages() {
    local topic=$1
    local from_beginning=${2:-"false"}
    
    if [ -z "$topic" ]; then
        print_error "Usage: ./kafka.sh consume <topic-name> [from-beginning]"
        exit 1
    fi
    
    local extra_args=""
    if [ "$from_beginning" == "true" ]; then
        extra_args="--from-beginning"
    fi
    
    print_info "Consuming from topic: $topic"
    print_info "Press Ctrl+C to stop"
    docker exec -it $KAFKA_CONTAINER kafka-console-consumer \
        --bootstrap-server $KAFKA_INTERNAL \
        --topic $topic \
        $extra_args \
        --property print.key=true \
        --property print.timestamp=true
}

produce_message() {
    local topic=$1
    if [ -z "$topic" ]; then
        print_error "Usage: ./kafka.sh produce <topic-name>"
        exit 1
    fi
    
    print_info "Producing to topic: $topic"
    print_info "Enter messages (one per line, Ctrl+D to finish):"
    docker exec -it $KAFKA_CONTAINER kafka-console-producer \
        --bootstrap-server $KAFKA_INTERNAL \
        --topic $topic
}

count_messages() {
    local topic=$1
    if [ -z "$topic" ]; then
        print_error "Usage: ./kafka.sh count <topic-name>"
        exit 1
    fi
    
    print_info "Counting messages in topic: $topic"
    docker exec $KAFKA_CONTAINER kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list $KAFKA_INTERNAL \
        --topic $topic \
        --time -1 | awk -F ":" '{sum += $3} END {print "Total messages: " sum}'
}

# =============================================================================
# Kafka Connect (for future use)
# =============================================================================

list_connectors() {
    print_info "Kafka Connect not configured yet"
    print_info "To enable: add kafka-connect service to docker-compose.yml"
}

# =============================================================================
# Kafka Streams State Stores
# =============================================================================

list_state_stores() {
    print_info "Listing Kafka Streams state stores..."
    print_info "State stores are managed by processors internally"
    print_info "Check Cassandra for persisted state"
}

# =============================================================================
# Monitoring & Health
# =============================================================================

check_broker_health() {
    print_info "Checking Kafka broker health..."
    docker exec $KAFKA_CONTAINER kafka-broker-api-versions \
        --bootstrap-server $KAFKA_INTERNAL
    print_success "Broker is healthy"
}

show_broker_config() {
    print_info "Showing broker configuration..."
    docker exec $KAFKA_CONTAINER kafka-configs \
        --bootstrap-server $KAFKA_INTERNAL \
        --describe \
        --entity-type brokers \
        --entity-name 1
}

show_lag() {
    print_info "Showing consumer lag for all groups..."
    for group in $(docker exec $KAFKA_CONTAINER kafka-consumer-groups \
        --bootstrap-server $KAFKA_INTERNAL \
        --list); do
        echo ""
        print_info "Group: $group"
        docker exec $KAFKA_CONTAINER kafka-consumer-groups \
            --bootstrap-server $KAFKA_INTERNAL \
            --describe \
            --group $group
    done
}

# =============================================================================
# Setup & Initialization
# =============================================================================

setup_all() {
    print_info "Setting up Kafka Monitor..."
    check_kafka
    create_topics
    print_success "Setup complete!"
    echo ""
    print_info "Created topics:"
    list_topics
    echo ""
    print_info "Next steps:"
    echo "  1. Start processors: docker-compose up -d"
    echo "  2. Check consumer groups: ./kafka.sh list-groups"
    echo "  3. Monitor messages: ./kafka.sh consume orders"
}

# =============================================================================
# Test Data Generation
# =============================================================================

generate_test_data() {
    local count=${1:-10}
    print_info "Generating $count test orders..."
    
    for i in $(seq 1 $count); do
        local customer_id=$((RANDOM % 100 + 1))
        local product_id="PROD-$((RANDOM % 20 + 1))"
        local quantity=$((RANDOM % 10 + 1))
        local unit_price=$((RANDOM % 100 + 10))
        local total=$((quantity * unit_price))
        local timestamp=$(date +%s)
        local statuses=("pending" "accepted" "denied")
        local status=${statuses[$((RANDOM % 4))]}
        
        local message="{\"order-id\":\"ORDER-TEST-$i\",\"customer-id\":$customer_id,\"product-id\":\"$product_id\",\"quantity\":$quantity,\"unit-price\":$unit_price.0,\"total\":$total.0,\"timestamp\":$timestamp,\"status\":\"$status\"}"
        
        echo "$message" | docker exec -i $KAFKA_CONTAINER kafka-console-producer \
            --bootstrap-server $KAFKA_INTERNAL \
            --topic orders
        
        echo "  ✓ Sent order: ORDER-TEST-$i"
    done
    
    print_success "Generated $count test orders"
}

# =============================================================================
# Help
# =============================================================================

show_help() {
    cat << EOF
Kafka Monitor - Kafka Management Script

USAGE:
    ./kafka.sh <command> [options]

SETUP:
    setup               Create topics and initialize Kafka
    
TOPIC MANAGEMENT:
    create-topics       Create all required topics
    list-topics         List all topics
    describe-topic      Describe a specific topic
    delete-topic        Delete a topic
    
CONSUMER GROUPS:
    list-groups         List all consumer groups
    describe-group      Describe a specific consumer group
    reset-group         Reset consumer group offsets
    
MESSAGES:
    consume             Consume messages from a topic
    produce             Produce messages to a topic
    count               Count messages in a topic
    
MONITORING:
    check-health        Check broker health
    show-config         Show broker configuration
    show-lag            Show consumer lag for all groups
    
TESTING:
    test-data           Generate test order data
    
EXAMPLES:
    ./kafka.sh setup
    ./kafka.sh consume orders true
    ./kafka.sh describe-group query-processor
    ./kafka.sh test-data 50
    ./kafka.sh show-lag

EOF
}

# =============================================================================
# Main Command Router
# =============================================================================

case "${1:-help}" in
    # Setup
    setup)
        setup_all
        ;;
    
    # Topics
    create-topics)
        check_kafka
        create_topics
        ;;
    list-topics)
        check_kafka
        list_topics
        ;;
    describe-topic)
        check_kafka
        describe_topic "$2"
        ;;
    delete-topic)
        check_kafka
        delete_topic "$2"
        ;;
    
    # Consumer Groups
    list-groups)
        check_kafka
        list_consumer_groups
        ;;
    describe-group)
        check_kafka
        describe_consumer_group "$2"
        ;;
    reset-group)
        check_kafka
        reset_consumer_group "$2"
        ;;
    
    # Messages
    consume)
        check_kafka
        consume_messages "$2" "$3"
        ;;
    produce)
        check_kafka
        produce_message "$2"
        ;;
    count)
        check_kafka
        count_messages "$2"
        ;;
    
    # Monitoring
    check-health)
        check_kafka
        check_broker_health
        ;;
    show-config)
        check_kafka
        show_broker_config
        ;;
    show-lag)
        check_kafka
        show_lag
        ;;
    
    # Testing
    test-data)
        check_kafka
        generate_test_data "$2"
        ;;
    
    # Help
    help|--help|-h)
        show_help
        ;;
    
    *)
        print_error "Unknown command: $1"
        echo ""
        show_help
        exit 1
        ;;
esac