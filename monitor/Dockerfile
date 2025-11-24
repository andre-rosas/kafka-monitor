FROM clojure:temurin-21-lein-2.11.2-jammy

RUN apt-get update && \
    apt-get install -y curl netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

COPY shared /app/shared
WORKDIR /app/shared
RUN lein install

COPY monitor /app/monitor
WORKDIR /app/monitor
RUN lein deps

EXPOSE 3000 3449 7002

CMD ["lein", "figwheel"]