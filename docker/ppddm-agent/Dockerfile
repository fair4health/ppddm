# Stage 0
FROM maven:3.6-openjdk-11 AS builder

ENV PPDDM_HOME /usr/local/ppddm
RUN mkdir -p "$PPDDM_HOME"
WORKDIR $PPDDM_HOME

COPY pom.xml .
COPY ppddm-agent .
COPY ppddm-core .
COPY ppddm-manager .

RUN mvn package

# Stage 1
FROM openjdk:11-jre-slim

ENV PPDDM_HOME /usr/local/ppddm
RUN mkdir -p "$PPDDM_HOME"
WORKDIR $PPDDM_HOME

COPY --from=builder $PPDDM_HOME/ppddm-agent/target/ppddm-agent-standalone.jar .

COPY docker/ppddm-agent/docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh

ENTRYPOINT ["./docker-entrypoint.sh"]
