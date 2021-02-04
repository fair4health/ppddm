# FAIR4Health Privacy-Preserving Distributed Data Mining (PPDDM) Framework

<p align="center">
  <a href="https://www.fair4health.eu" target="_blank">
  <img width="400" src="https://www.fair4health.eu/images/logo.png" alt="FAIR4Health logo"></a>
</p>

<p align="center">
  <a href="https://github.com/fair4health/ppddm">
  <img src="https://img.shields.io/github/license/fair4health/data-curation-tool" alt="License"></a>
</p>

## About

This is the data mining framework of the FAIR4Health Project (https://www.fair4health.eu/) 
which allows data scientists to train statistical/machine learning models using the 
FAIRified datasets of different healthcare data and health research data sources.

This framework is composed of two main modules:
 * **ppddm-agent**
 * **ppddm-manager**
 
And supporting modules which are referenced from the ppddm-agent and ppddm-manager modules:
 * **ppddm-core**

### ppddm-agent
Privacy-Preserving Distributed Data Mining (PPDDM) Agent
 * An Agent is a part of the FAIR4Health Agent.
 * Agents are deployed within the boundaries of data source and the services exposed by the Agent 
 can only be accessed by the PPDDM Manager
 * An Agent requires an HL7 FHIR Repository to extract data to be used for the machine learning algorithms.
 * ML algorithms are executed using the data extracted from the FHIr Repository and only the trained models
 leave the data source boundaries. No data is allowed to leave.

### ppddm-manager
Privacy-Preserving Distributed Data Mining (PPDDM) Manager 
 * The Manager is a part of the FAIR4Health Platform.
 * The Manager needs to be deployed centrally (possibly as a Cloud service) so that it can interact with
 all Agents.
 * The Manager orchestrates the execution of the training algorithms in the distributed environment
 of several Agents.
 * The Manager creates a common final training model by executing the algorithms on each Agent sequentially. 
 * The services of the Manager is to be consumed by a data scientist and the data scientist will be using a
 web-based GUI

### ppddm-core
Core components and libraries shared by other modules of the FAIR4Health PPDDM such as the REST model
or machine learning libraries.

## Warning
 * This project utilizes Apacke Spark and Spark has a dependency on Windows OS as described 
   on [Stackoverflow](https://stackoverflow.com/questions/35652665/java-io-ioexception-could-not-locate-executable-null-bin-winutils-exe-in-the-ha). 
   If you will use on Windows, you need the winutils as desribed in the link.
 * Tests of the ppddm-agent requires an HL7 FHIR Repository (prefereably an [onFHIR Repository](https://onfhir.io)) 
   to be running at the host and port configured in the application.conf of the Agent.

# Deployment (Docker)

## PPDDM-Agent Deployment (Docker)

Privacy-Preserving Distributed Data Mining (PPDDM) Agent is the agent part of the distributed data mining 
framework of the FAIR4Health Project. A PPDDM-Agent **requires** an [HL7 FHIR](https://www.hl7.org/fhir/) 
Repository which implements the [FHIRPath specification](https://www.hl7.org/fhir/fhirpath.html). 
[onFHIR.io](https://onfhir.io/) is the FHIR Repository used within the FAIR4Health Project, hence 
PPDDM-Agent runs peacefully on an onFHIR instance.

This image contains the ppddm-agent under the [ppddm](https://github.com/fair4health/ppddm) framework of the 
FAIR4Health project. To run a ppddm-agent, the required onFHIR instance can be used as another container which 
is also available on [Dockerhub](https://hub.docker.com/r/fair4health/onfhir).

### How to use PPDDM-Agent image

Since this image requires another container (which runs an onFHIR instance), the most convenient way to run 
is a `docker-compose` file.

	version: '2'
	services:
	  mongo:
	    image: mongo:4.4
	    volumes:
	      - './volumes/mongo:/data/db'
	    ports:
	      - 27017:27017
	    container_name: f4h-mongo
	    restart: always
	  onfhir:
	    image: fair4health/onfhir:latest
	    depends_on:
	      - mongo
	    environment:
	      APP_CONF_FILE: 'fair4health.conf'
	      LOGBACK_CONF_FILE: 'logback.xml'
	      FHIR_ROOT_URL: 'https://f4h.srdc.com.tr/fhir'
	      DB_EMBEDDED: 'false'
	      DB_HOST: 'f4h-mongo:27017'
	    ports:
	      - 8080:8080
	    container_name: f4h-onfhir
	    restart: always
	  ppddm-agent:
	    image: fair4health/ppddm-agent:latest
	    depends_on:
	      - onfhir
	    environment:
	      AGENT_ID: agent-srdc
	      SERVER_PORT: 8001
	      FHIR_HOST: f4h-onfhir
	      FHIR_PORT: 8080
	    ports:
	      - 8001:8001
	    volumes:
	      - './volumes/ppddm-agent-srdc:/usr/local/ppddm/ppddm-store'
	    container_name: f4h-ppddm-agent-srdc
	    restart: always

### Environment Variables

The PPDDM-Agent image of the FAIR4Health uses several environment variables.

**APP_NAME**: This environment variable can be used to set the name of the ppddm-agent instance. 
It is also used as the [Spark](http://spark.apache.org/) App Name.

**SERVER_HOST**: Hostname (or IP address) that the ppddm-agent will bind. Using 0.0.0.0 will bind the server to 
both localhost and the IP of the server that you deploy it.

**SERVER_PORT**: Port number that the ppddm-agent will listen.

**SERVER_BASEURI**: Base URI for the ppddm-agent e.g. With the default configuration, the root path of the 
ppddm-agent server will be `http://localhost:8000/agent` in which `/agent` is the `SERVER_BASEURI`.

**AGENT_ID**: Unique (within the PPDDM framework) identifier for this ppddm-agent so that it can be identified 
among other ppddm-agents.

**FHIR_PROTOCOL**: `http` or `https`, the protocol used by the FHIR server to be accessed.

**FHIR_HOST**: The hostname of the FHIR server to be accessed.

**FHIR_PORT**: The port number of the FHIR server to be accessed.

**FHIR_BASEURI**: The base URI of the FHIR server to be accessed.

**AUTH_ENABLED**: If it is enabled, PPDDM-Agent requires authentication of the client (i.e. the PPDDM-Manager) who 
invokes the services of this PPDDM-Agent instance.

**AUTH_CLIENT_ID**: The identifier (username) of the client to be authenticated by this PPDDM-Agent instance.

**AUTH_CLIENT_SECRET**: The secret (password) of the client to be authenticated by this PPDDM-Agent instance.

## PPDDM-Manager Deployment (Docker)

Privacy-Preserving Distributed Data Mining (PPDDM) Manager is the manager part of the distributed data mining 
framework of the FAIR4Health Project, manager is the coordinator of the PPDDM-Agent instances. 
A PPDDM-Manager needs a MongoDB as its backend database, however it is possible to start an embedded MongoDB 
while starting up the PPDDM-Manager using the appropriate configuration parameters.

This image contains the ppddm-manager under the [ppddm](https://github.com/fair4health/ppddm) framework of the 
FAIR4Health project.

### How to use PPDDM-Manager image

This image requires MongoDB (either started embedded or as another Docker container) and may require PPDDM-Agent container(s) 
based on your preference, hence the most convenient way to run is a `docker-compose` file. This example starts three
different PPDDM-Agent instances on the very same onFHIR repository.

	version: '2'
	services:
	  mongo:
	    image: mongo:4.4
	    volumes:
	      - './volumes/mongo:/data/db'
	    ports:
	      - 27077:27017
	    container_name: f4h-mongo
	    restart: always
	  onfhir:
	    image: fair4health/onfhir:latest
	    depends_on:
	      - mongo
	    environment:
	      APP_CONF_FILE: 'fair4health.conf'
	      LOGBACK_CONF_FILE: 'logback.xml'
	      FHIR_ROOT_URL: 'https://f4h.srdc.com.tr/fhir'
	      DB_EMBEDDED: 'false'
	      DB_HOST: 'f4h-mongo:27017'
	    ports:
	      - 7070:8080
	    container_name: f4h-onfhir
	    restart: always
	  ppddm-manager:
	    image: fair4health/ppddm-manager:latest
	    depends_on:
	      - mongo
	      - ppddm-agent-1
	      - ppddm-agent-2
	      - ppddm-agent-3
	    environment:
	      SERVER_PORT: 8000
	      AGENTS_DEFINITION: 'agents-deployment.json'
	      MONGO_EMBEDDED: 'false'
	      MONGO_HOST: f4h-mongo
	      MONGO_PORT: 27017
	    ports:
	      - 7000:8000
	    container_name: f4h-ppddm-manager
	    restart: always
	  ppddm-agent-1:
	    image: fair4health/ppddm-agent:latest
	    depends_on:
	      - onfhir
	    environment:
	      AGENT_ID: agent-srdc-1
	      SERVER_PORT: 7001
	      FHIR_HOST: f4h-onfhir
	      FHIR_PORT: 8080
	    volumes:
	      - './volumes/ppddm-agent1:/usr/local/ppddm/ppddm-store'
	    container_name: f4h-ppddm-agent-1
	    restart: always
	  ppddm-agent-2:
	    image: fair4health/ppddm-agent:latest
	    depends_on:
	      - onfhir
	    environment:
	      AGENT_ID: agent-srdc-2
	      SERVER_PORT: 7002
	      FHIR_HOST: f4h-onfhir
	      FHIR_PORT: 8080
	    volumes:
	      - './volumes/ppddm-agent2:/usr/local/ppddm/ppddm-store'
	    container_name: f4h-ppddm-agent-2
	    restart: always
	  ppddm-agent-3:
	    image: fair4health/ppddm-agent:latest
	    depends_on:
	      - onfhir
	    environment:
	      AGENT_ID: agent-srdc-3
	      SERVER_PORT: 7003
	      FHIR_HOST: f4h-onfhir
	      FHIR_PORT: 8080
	    volumes:
	      - './volumes/ppddm-agent3:/usr/local/ppddm/ppddm-store'
	    container_name: f4h-ppddm-agent-3
	    restart: always

### Environment Variables

The PPDDM-Manager image of the FAIR4Health uses several environment variables.

**APP_NAME**: This environment variable can be used to set the name of the ppddm-manager instance.
It is also used as the [Spark](http://spark.apache.org/) App Name.

**SERVER_HOST**: Hostname (or IP address) that the ppddm-manager will bind. Using 0.0.0.0 will bind the server to
both localhost and the IP of the server that you deploy it.

**SERVER_PORT**: Port number that the ppddm-manager will listen.

**SERVER_BASEURI**: Base URI for the ppddm-manager e.g. With the default configuration, the root path of the
ppddm-manager server will be `http://localhost:8000/manager` in which `/manager` is the `SERVER_BASEURI`.

**MONGO_EMBEDDED**: ppddm-manager requires a MongoDB instance. The application can be run on an embedded MongoDB 
instance if no external MondoDB is configured through the `MONGO_HOST` variable. If `MONGO_EMBEDDED` is true, then 
the MongoDB instance will be automatically started on `MONGO_HOST:MONGO_PORT` values.

**MONGO_HOST**: Host for the MongoDB database. Default is `localhost`.

**MONGO_PORT**: Port number for the MongoDB database. Defaule is `27017`.

**MONGO_DBNAME**: The name of the database to be created for running the ppddm-manager.

**AUTH_CLIENT_ID**: The identifier (username) to get this ppddm-manager authenticated by the ppddm-agent instances 
as a client.

**AUTH_CLIENT_SECRET**: The secret (password) to get this ppddm-manager authenticated by the ppddm-agent instances
as a client.

**AUTH_ENABLED**: If it is enabled, PPDDM-Manager authenticates itself on the provided AUTH_SERVER_HOST so that
any received service invocations to this ppddm-manager instance are authenticated on the AUTH_SERVER_HOST 
using token authentication for token introspection.

**AUTH_SERVER_HOST**: The authentication server to authenticate/introspect the received service invocations.

**AUTH_SERVER_USERNAME**: The username to authenticate this ppddm-manager instance on the AUTH_SERVER_HOST. Only after 
this ppddm-manager instance is authenticated, it can introspect the tokens within the received requests.

**AUTH_SERVER_PASSWORD**: The username to authenticate this ppddm-manager instance on the AUTH_SERVER_HOST.

**AUTH_SERVER_LOGINPATH**: The path to be appended to the AUTH_SERVER_HOST to authenticate this ppddm-manager instance.

**AUTH_SERVER_INTROSPECTIONPATH**: The path to be appended to the AUTH_SERVER_HOST to introspect the tokens
received with the requests.

## Acknowledgement

This research has received funding from the European Union’s Horizon 2020 research and innovation programme under grant agreement No 824666,
[FAIR4Health Project](https://www.fair4health.eu/) (Improving Health Research in EU through FAIR Data).
