# Stage 0
FROM maven:3.6-openjdk-11 AS builder

ENV PPDDM_HOME /usr/local/ppddm
RUN mkdir -p "$PPDDM_HOME"
WORKDIR $PPDDM_HOME

COPY pom.xml .
COPY ppddm-agent ./ppddm-agent
COPY ppddm-core ./ppddm-core
COPY ppddm-manager ./ppddm-manager

# Build without running the unit and integration tests (because integration tests require an onFHIR Repository)
RUN mvn package -Pxtest

# Stage 1
FROM openjdk:11-jre-slim

ENV PPDDM_HOME /usr/local/ppddm
RUN mkdir -p "$PPDDM_HOME"
WORKDIR $PPDDM_HOME

COPY --from=builder $PPDDM_HOME/ppddm-manager/target/ppddm-manager-standalone.jar .

COPY docker/ppddm-manager/agents-deployment.json .

COPY docker/ppddm-manager/docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh

ENTRYPOINT ["./docker-entrypoint.sh"]
