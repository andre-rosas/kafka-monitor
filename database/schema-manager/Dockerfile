FROM ubuntu:22.04

# Install cqlsh and dependencies
RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install cqlsh

# Copy migration scripts
COPY migrations /migrations

# Copy and make the migration script executable
COPY run-migrations.sh /run-migrations.sh
RUN chmod +x /run-migrations.sh

CMD ["/run-migrations.sh"]