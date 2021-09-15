#!/usr/bin/env bash

JAVA_CMD="java -Xms2g -Xmx8g -jar "

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

if [ ! -z "$AGENT_ID" ]; then
    JAVA_CMD+="-Dagent.id=$AGENT_ID "
fi
if [ ! -z "$AGENT_ENDPOINT" ]; then
    JAVA_CMD+="-Dagent.endpoint=$AGENT_ENDPOINT "
fi
if [ ! -z "$AGENT_BATCHSIZE" ]; then
    JAVA_CMD+="-Dagent.batch-size=$AGENT_BATCHSIZE "
fi
if [ ! -z "$DATA_PREPARATION_TIMEOUT" ]; then
    JAVA_CMD+="-Dagent.data-preparation-timeout=$DATA_PREPARATION_TIMEOUT "
fi
if [ ! -z "$ASSOCIATION_MAX_ITEM_COUNT" ]; then
    JAVA_CMD+="-Dagent.dm.association.max-item-count=$ASSOCIATION_MAX_ITEM_COUNT "
fi

if [ ! -z "$SPARK_MASTER" ]; then
    JAVA_CMD+="-Dspark.master=$SPARK_MASTER "
fi

if [ ! -z "$FHIR_PROTOCOL" ]; then
    JAVA_CMD+="-Dfhir.protocol=$FHIR_PROTOCOL "
fi
if [ ! -z "$FHIR_HOST" ]; then
    JAVA_CMD+="-Dfhir.host=$FHIR_HOST "
fi
if [ ! -z "$FHIR_PORT" ]; then
    JAVA_CMD+="-Dfhir.port=$FHIR_PORT "
fi
if [ ! -z "$FHIR_BASEURI" ]; then
    JAVA_CMD+="-Dfhir.base-uri=$FHIR_BASEURI "
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

if [ ! -z "$AUTH_ENABLED" ]; then
    JAVA_CMD+="-Dauth.enabled=$AUTH_ENABLED "
fi
if [ ! -z "$AUTH_CLIENT_ID" ]; then
    JAVA_CMD+="-Dauth.client.id=$AUTH_CLIENT_ID "
fi
if [ ! -z "$AUTH_CLIENT_SECRET" ]; then
    JAVA_CMD+="-Dauth.client.secret=$AUTH_CLIENT_SECRET "
fi

# Delay the execution for this amount of seconds
if [ ! -z "$DELAY_EXECUTION" ]; then
    sleep $DELAY_EXECUTION
fi

# Finally, tell which jar to run
JAVA_CMD+="ppddm-agent-standalone.jar"

eval $JAVA_CMD
