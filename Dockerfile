# Dockerfile for Kafka Monitor
FROM eclipse-temurin:21-jre

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create application directory
WORKDIR /app

# Copy project files
COPY project.clj .
COPY shared/ shared/
COPY monitor/ monitor/
COPY order-processor/ order-processor/
COPY query-processor/ query-processor/
COPY registry-processor/ registry-processor/

# Run kafka.sh on render (Comment this if you use local)
COPY kafka.sh .
RUN chmod +x ./kafka.sh

# Install Leiningen
RUN curl -o /usr/local/bin/lein https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein && \
    chmod +x /usr/local/bin/lein

# Download dependencies
RUN cd shared && lein install

# Build application
RUN lein sub deps
RUN lein sub compile

# Build the monitor frontend
RUN cd monitor && lein cljsbuild once min

# Create non-root user
RUN useradd -m -u 1001 appuser && chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 3000

# Command to start the application
CMD ["lein", "run-monitor"]