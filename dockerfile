# Stage 1: Build the Java code
FROM gradle:8-jdk17 AS builder
WORKDIR /app
COPY . /app/
RUN sed -i 's/\r$//' gradlew && ./gradlew \
    :clients:jar \
    :connect:api:jar \
    :connect:runtime:jar \
    :connect:json:jar \
    :connect:mirror:jar \
    :connect:mirror-client:jar \
    :connect:mirror:copyDependantLibs \
    -x test

# Stage 2: Create the actual Kafka Image
FROM apache/kafka:4.0.0
USER root

COPY --from=builder /app/clients/build/libs/kafka-clients-*.jar /opt/kafka/libs/
COPY --from=builder /app/connect/api/build/libs/connect-api-*.jar /opt/kafka/libs/
COPY --from=builder /app/connect/runtime/build/libs/connect-runtime-*.jar /opt/kafka/libs/
COPY --from=builder /app/connect/json/build/libs/connect-json-*.jar /opt/kafka/libs/
COPY --from=builder /app/connect/mirror/build/libs/connect-mirror-*.jar /opt/kafka/libs/
COPY --from=builder /app/connect/mirror-client/build/libs/connect-mirror-client-*.jar /opt/kafka/libs/
COPY --from=builder /app/connect/mirror/build/dependant-libs/*.jar /opt/kafka/libs/

RUN rm -f /opt/kafka/libs/connect-mirror-4.0.0.jar \
          /opt/kafka/libs/connect-mirror-client-4.0.0.jar \
          /opt/kafka/libs/kafka-clients-4.0.0.jar \
          /opt/kafka/libs/connect-api-4.0.0.jar \
          /opt/kafka/libs/connect-runtime-4.0.0.jar \
          /opt/kafka/libs/connect-json-4.0.0.jar

USER appuser