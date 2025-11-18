# registry-processor/Dockerfile
FROM clojure:temurin-21-lein AS builder

WORKDIR /app

# Install shared dependencies first
COPY shared ./shared
RUN cd shared && lein install

# Copy registry-processor specific files
COPY registry-processor/project.clj ./
COPY registry-processor/src ./src
COPY registry-processor/resources ./resources

# Build application
RUN lein deps
RUN lein uberjar

# Runtime stage
FROM eclipse-temurin:21-jre-jammy

# Install curl for health checks
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Create application user for security
RUN groupadd --system appuser && \
    useradd --system --uid 1001 --gid appuser --home /app --shell /sbin/nologin appuser

WORKDIR /app

# Copy built application from builder stage
COPY --from=builder /app/target/uberjar/registry-processor-standalone.jar /app/registry-processor.jar

# Set proper ownership
RUN chown -R appuser:appuser /app

USER appuser

# Environment variables with defaults
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9093
ENV CASSANDRA_HOST=cassandra
ENV CASSANDRA_PORT=9042
ENV CASSANDRA_DATACENTER=datacenter1
ENV PROCESSOR_ID=registry-processor-1
ENV PROFILE=prod

# Health check configuration
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Application entry point with JVM optimizations for containers
ENTRYPOINT ["java", \
    "-XX:+UseContainerSupport", \
    "-XX:MaxRAMPercentage=75.0", \
    "-Dprofile=prod", \
    "-jar", \
    "/app/registry-processor.jar"]