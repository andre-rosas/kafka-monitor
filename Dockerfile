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
COPY resources/ resources/
COPY src/ src/
COPY test/ test/

# Install Leiningen
RUN curl -o /usr/local/bin/lein https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein && \
    chmod +x /usr/local/bin/lein

# Download dependencies
RUN lein deps

# Build application
RUN lein compile
RUN lein cljsbuild once min

# Create non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 3000

# Command to start the application
CMD ["lein", "run"]