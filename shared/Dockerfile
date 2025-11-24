FROM clojure:lein-2.11.2 AS builder

WORKDIR /app

# Copy project file
COPY project.clj .

# Download dependencies
RUN lein deps

# Copy source code
COPY src/ ./src/

# Compile
RUN lein compile

# Final stage
FROM clojure:lein-2.11.2

WORKDIR /app
COPY --from=builder /app /app

CMD ["lein", "repl"]