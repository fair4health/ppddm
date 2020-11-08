FROM openjdk:11-jdk

ENV PPDDM_HOME /usr/local/ppddm
RUN mkdir -p "$PPDDM_HOME"

WORKDIR $PPDDM_HOME

COPY ppddm-manager-standalone.jar .
COPY application.conf .
COPY agents-deployment.json .

COPY docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh

ENTRYPOINT ["./docker-entrypoint.sh"]
