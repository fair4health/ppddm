#!/usr/bin/env bash

JAVA_CMD="java -jar "

if [ ! -z "$APP_NAME" ]; then
    JAVA_CMD+="-Dapp.name=$DEPLOYMENT_NAME "
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

if [ ! -z "$AGENTS_DEFINITION" ]; then
    JAVA_CMD+="-Dagents.definition-path=$AGENTS_DEFINITION "
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

# Delay the execution for this amount of seconds
if [ ! -z "$DELAY_EXECUTION" ]; then
    sleep $DELAY_EXECUTION
fi

# Finally, tell which jar to run
JAVA_CMD+="ppddm-manager-standalone.jar"

eval $JAVA_CMD