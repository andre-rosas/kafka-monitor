# shared/Dockerfile
FROM clojure:lein-2.11.2

WORKDIR /app
COPY . .

# Install development dependencies
RUN lein deps

# Run tests with coverage
RUN lein cloverage --fail-threshold 80

# Run normal tests as well
RUN lein test

# Install the package
RUN lein install