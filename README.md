# FAIR4Health Data Curation & Validation Tool

<p align="center">
  <a href="https://www.fair4health.eu" target="_blank"><img width="400" src="https://www.fair4health.eu/images/logo.png" alt="FAIR4Health logo"></a>
</p>

<p align="center">
  <a href="https://github.com/fair4health/ppddm"><img src="https://img.shields.io/github/license/fair4health/data-curation-tool" alt="License"></a>
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

## Acknowledgement

This research has received funding from the European Union’s Horizon 2020 research and innovation programme under grant agreement No 824666,
[FAIR4Health Project](https://www.fair4health.eu/) (Improving Health Research in EU through FAIR Data).
