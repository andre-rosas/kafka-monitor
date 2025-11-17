# =============================================================================
# Query Processor Dockerfile
# =============================================================================
# Multi-stage build for query-processor microservice
# Stage 1: Build uberjar with Leiningen
# Stage 2: Run with minimal JRE

# =============================================================================
# Stage 1: Build
# =============================================================================
FROM clojure:temurin-21-lein AS builder

WORKDIR /app

COPY shared ./shared
RUN cd shared && lein install

COPY query-processor/project.clj ./
COPY query-processor/src ./src
COPY query-processor/resources ./resources

RUN lein deps
RUN lein uberjar

FROM eclipse-temurin:21-jre-jammy

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install required tools
# RUN apk add --no-cache curl

# Create app user (security best practice)
# RUN addgroup -g 1001 -S appuser && \
#     adduser -u 1001 -S appuser -G appuser

RUN groupadd --system appuser && \
    useradd --system --uid 1001 --gid appuser --home /app --shell /sbin/nologin appuser

WORKDIR /app

# Copy uberjar from builder stage
COPY --from=builder /app/target/uberjar/query-processor-standalone.jar /app/query-processor.jar

# Change ownership
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Environment variables (can be overridden at runtime)
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9093
ENV CASSANDRA_HOST=cassandra
ENV CASSANDRA_PORT=9042
ENV CASSANDRA_DATACENTER=datacenter1
ENV PROCESSOR_ID=query-processor-1
ENV PROFILE=prod

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Expose port (if adding HTTP API later)
# EXPOSE 8080

# Run application
ENTRYPOINT ["java", \
    "-XX:+UseContainerSupport", \
    "-XX:MaxRAMPercentage=75.0", \
    "-Dprofile=prod", \
    "-Dlogback.configurationFile=logback.xml", \
    "-jar", \
    "/app/query-processor.jar"]

# =============================================================================
# Build and Run Instructions
# =============================================================================
# Build:
#   docker build -t query-processor:latest .
#
# Run (standalone):
#   docker run --rm \
#     -e KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
#     -e CASSANDRA_HOST=localhost \
#     query-processor:latest
#
# Run (with docker-compose):
#   See docker-compose.yml in project root
# =============================================================================