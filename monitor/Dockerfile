FROM clojure:temurin-21-lein AS builder
WORKDIR /app

COPY shared ./shared
RUN cd shared && lein install

COPY monitor/project.clj ./
COPY monitor/src ./src
COPY monitor/resources ./resources

RUN lein deps
RUN lein cljsbuild once min
RUN lein uberjar

FROM eclipse-temurin:21-jre-jammy
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
RUN groupadd --system appuser && \
    useradd --system --uid 1001 --gid appuser --home /app --shell /sbin/nologin appuser

WORKDIR /app

COPY --from=builder /app/target/uberjar/monitor-standalone.jar /app/monitor.jar
COPY --from=builder /app/resources /app/resources

RUN chown -R appuser:appuser /app
USER appuser

ENV CASSANDRA_HOST=cassandra
ENV CASSANDRA_PORT=9042
ENV PORT=8080
ENV PROFILE=prod

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080/api/health || exit 1

EXPOSE 8080

ENTRYPOINT ["java", \
    "-XX:+UseContainerSupport", \
    "-XX:MaxRAMPercentage=75.0", \
    "-Dprofile=prod", \
    "-jar", \
    "/app/monitor.jar"]