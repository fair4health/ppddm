#!/usr/bin/env bash

JAVA_CMD="java -jar "

if [ ! -z "$APP_NAME" ]; then
    JAVA_CMD+="-Dapp.name=$APP_NAME "
fi

if [ ! -z "$SERVER_HOST" ]; then
    JAVA_CMD+="-Dserver.host=$SERVER_HOST "
fi
if [ ! -z "$SERVER_PORT" ]; then
    JAVA_CMD+="-Dserver.port=$SERVER_PORT "
fi
if [ ! -z "$SERVER_BASEURI" ]; then
    JAVA_CMD+="-Dserver.base-uri=$SERVER_BASEURI "
fi

if [ ! -z "$SPARK_MASTER" ]; then
    JAVA_CMD+="-Dspark.master=$SPARK_MASTER "
fi

if [ ! -z "$AUTH_ENABLED" ]; then
    JAVA_CMD+="-Dauth.enabled=$AUTH_ENABLED "
fi
if [ ! -z "$AUTH_CLIENT_ID" ]; then
    JAVA_CMD+="-Dauth.client.id=$AUTH_CLIENT_ID "
fi
if [ ! -z "$AUTH_CLIENT_SECRET" ]; then
    JAVA_CMD+="-Dauth.client.secret=$AUTH_CLIENT_SECRET "
fi
if [ ! -z "$AUTH_SERVER_HOST" ]; then
    JAVA_CMD+="-Dauth.server.host=$AUTH_SERVER_HOST "
fi
if [ ! -z "$AUTH_SERVER_USERNAME" ]; then
    JAVA_CMD+="-Dauth.server.username=$AUTH_SERVER_USERNAME "
fi
if [ ! -z "$AUTH_SERVER_PASSWORD" ]; then
    JAVA_CMD+="-Dauth.server.password=$AUTH_SERVER_PASSWORD "
fi
if [ ! -z "$AUTH_SERVER_LOGINPATH" ]; then
    JAVA_CMD+="-Dauth.server.login.path=$AUTH_SERVER_LOGINPATH "
fi
if [ ! -z "$AUTH_SERVER_INTROSPECTIONPATH" ]; then
    JAVA_CMD+="-Dauth.server.introspection.path=$AUTH_SERVER_INTROSPECTIONPATH "
fi

if [ ! -z "$AGENTS_DEFINITION" ]; then
    JAVA_CMD+="-Dagents.definition-path=$AGENTS_DEFINITION "
fi

if [ ! -z "$ORCHESTRATOR_SCHEDULE_INTERVAL" ]; then
    JAVA_CMD+="-Ddm.orchestrator.schedule.interval=$ORCHESTRATOR_SCHEDULE_INTERVAL "
fi

if [ ! -z "$MONGO_EMBEDDED" ]; then
    JAVA_CMD+="-Dmongodb.embedded=$MONGO_EMBEDDED "
fi
if [ ! -z "$MONGO_HOST" ]; then
    JAVA_CMD+="-Dmongodb.host=$MONGO_HOST "
fi
if [ ! -z "$MONGO_PORT" ]; then
    JAVA_CMD+="-Dmongodb.port=$MONGO_PORT "
fi
if [ ! -z "$MONGO_DBNAME" ]; then
    JAVA_CMD+="-Dmongodb.db=$MONGO_DBNAME "
fi

if [ ! -z "$AKKA_HTTP_REQUEST_TIMEOUT" ]; then
    JAVA_CMD+="-Dakka.http.server.request-timeout=$AKKA_HTTP_REQUEST_TIMEOUT "
fi
if [ ! -z "$AKKA_HTTP_PARSING_MAXCONTENTLENGTH" ]; then
    JAVA_CMD+="-Dakka.http.server.parsing.max-content-length=$AKKA_HTTP_PARSING_MAXCONTENTLENGTH "
fi

# Delay the execution for this amount of seconds
if [ ! -z "$DELAY_EXECUTION" ]; then
    sleep $DELAY_EXECUTION
fi

# Finally, tell which jar to run
JAVA_CMD+="ppddm-manager-standalone.jar"

eval $JAVA_CMD
