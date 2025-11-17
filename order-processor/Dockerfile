FROM clojure:temurin-21-lein AS builder

WORKDIR /app

# 1. Copy and install shared
COPY shared ./shared
RUN cd shared && lein install

# 2. copy only files from order-processors
COPY order-processor/project.clj ./
COPY order-processor/src ./src
COPY order-processor/resources ./resources

# 3. Compiles
RUN lein deps
RUN lein uberjar

FROM eclipse-temurin:21-jre-jammy

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

RUN groupadd --system appuser && \
    useradd --system --uid 1001 --gid appuser --home /app --shell /sbin/nologin appuser

WORKDIR /app

COPY --from=builder /app/target/uberjar/order-processor-standalone.jar /app/order-processor.jar

RUN chown -R appuser:appuser /app

USER appuser

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9093
ENV CASSANDRA_HOST=cassandra
ENV CASSANDRA_PORT=9042
ENV CASSANDRA_DATACENTER=datacenter1
ENV PRODUCER_ID=order-processor-1
ENV PROFILE=prod

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

EXPOSE 8080

ENTRYPOINT ["java", \
    "-XX:+UseContainerSupport", \
    "-XX:MaxRAMPercentage=75.0", \
    "-Dprofile=prod", \
    "-jar", \
    "/app/order-processor.jar"]