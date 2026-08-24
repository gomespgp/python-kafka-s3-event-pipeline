FROM confluentinc/cp-kafka-connect-base:7.6.0

USER root

# Install S3 Sink Connector into confluent-hub-components and set permissions
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.13 \
    && chmod -R 755 /usr/share/confluent-hub-components

USER appuser