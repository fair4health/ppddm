package ppddm.agent.controller.prepare

import java.util.concurrent.TimeUnit
import java.time.ZonedDateTime

import akka.Done
import com.typesafe.scalalogging.Logger
import io.onfhir.path._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.json4s.JsonAST.JObject
import org.json4s.{JArray, JString}
import ppddm.agent.Agent
import ppddm.agent.config.AgentConfig
import ppddm.agent.spark.NodeExecutionContext._
import ppddm.agent.store.AgentDataStoreManager
import ppddm.core.fhir.{FHIRClient, FHIRQuery}
import ppddm.core.rest.model._
import ppddm.core.util.DataPreparationUtil
import ppddm.core.util.JsonFormatter._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, TimeoutException}
import scala.util.{Failure, Try}


/**
 * Controller object for data preparation.
 *
 * PPDDM Manager requests data preparation together with the unique identifier of the dataset on the platform.
 * After the submission, the manager checks whether the data is prepared or not by asking for the results.
 */
object DataPreparationController {

  private val logger: Logger = Logger(this.getClass)

  private val batchSize: Int = AgentConfig.agentBatchSize
  private implicit val sparkSession: SparkSession = Agent.dataMiningEngine.sparkSession

  import sparkSession.implicits._

  /**
   * Start the data preparation (data extraction process) with the given DataPreparationRequest.
   * This function successfully returns if the preparation request is started. Data preparation will continue in the background
   * and the clients will ask about the execution status through a separate API call.
   *
   * @param dataPreparationRequest The request object for the data preparation.
   * @return
   */
  def startPreparation(dataPreparationRequest: DataPreparationRequest): Future[Done] = {
    logger.debug("Data preparation request received.")

    if (dataPreparationRequest.featureset.variables.isEmpty) {
      throw DataPreparationException(s"The Featureset in the submitted DataPreparationRequest does not include any Variable definitions.")
    }
    if (dataPreparationRequest.eligibility_criteria.count(_.fhir_query.startsWith("/Patient")) > 1) { // Check if there are multiple Patient queries
      throw DataPreparationException(s"The Eligibility Criteria in the submitted DataPreparationRequest has multiple Patient queries. It should not be more than one.")
    }

    if (AgentDataStoreManager.getDataFrame(AgentDataStoreManager.getDatasetPath(dataPreparationRequest.dataset_id)).isDefined) {
      // Check data store whether the Dataset with given dataset_id is already created and saved
      Future {
        logger.warn(s"Dataset with id: ${dataPreparationRequest.dataset_id} already exists. Why do you want to prepare it again?")
        Done
      }
    } else {
      val fhirClientMaster = FHIRClient(AgentConfig.fhirHost, AgentConfig.fhirPort, AgentConfig.fhirBaseUri, AgentConfig.fhirProtocol)

      // Start with the Patients
      val patientQuery = QueryHandler.getPatientQuery(dataPreparationRequest.eligibility_criteria)

      patientQuery.getCount(fhirClientMaster) map { numOfPatients => //Count the resulting resources in terms of patients
        logger.debug(s"Number of patients: ${numOfPatients}")
        if (numOfPatients > 0) {
          //Number of pages to get all the results according to batch size
          val numOfReturnPagesForQuery = numOfPatients / batchSize + 1
          logger.debug(s"Number of workers to be run in parallel in Spark: ${numOfReturnPagesForQuery}")

          //Parallelize the execution and process pages in parallel
          val rdd: RDD[Try[Seq[Row]]] = sparkSession.sparkContext.parallelize(1 to numOfReturnPagesForQuery).mapPartitions(partitionIterator => {
            partitionIterator.map { pageIndex =>
              Try {
                // Instantiate a FHIRClient and FhirPathEvaluator for each worker node
                val fhirClientPartition = FHIRClient(AgentConfig.fhirHost, AgentConfig.fhirPort, AgentConfig.fhirBaseUri, AgentConfig.fhirProtocol)
                val fhirPathEvaluator = FhirPathEvaluator()

                // Fetch the Patient resources from the FHIR Repository and collect their IDs
                val theFuture = findEligiblePatients(fhirClientPartition, fhirPathEvaluator, dataPreparationRequest.eligibility_criteria, patientQuery, pageIndex) flatMap { eligiblePatientURIs =>
                  if (eligiblePatientURIs.isEmpty) {
                    // No patients are eligible in the partition, so return an empty Row
                    Future {
                      Seq.empty[Row]
                    }
                  } else {
                    if (dataPreparationRequest.featureset.variables.isEmpty) {
                      logger.warn("The feature set definition of the data preparation request does not contain any variable definitions. " +
                        "This is probably an error. DataPreparationRequest object should have been verified upto this point.")
                      Future {
                        Seq.empty[Row]
                      }
                    } else {
                      // Check if the featureset variables include an encounter type variable
                      if (hasEncounterTypeVariable(dataPreparationRequest.featureset)) {
                        val encounterCriteria = dataPreparationRequest.eligibility_criteria.filter(_.fhir_query.startsWith("/Encounter"))
                        findAllEligibleEncounters(fhirClientPartition, eligiblePatientURIs, encounterCriteria)
                          .flatMap { encounterMap: Map[String, EncounterBasedItem] =>
                            populateVariableValues(fhirClientPartition, fhirPathEvaluator, dataPreparationRequest.featureset, encounterMap.keySet, Option(encounterMap))
                              .map { resourceMap: Map[String, Map[String, Any]] =>
                                convertToSparkRow(dataPreparationRequest.featureset, encounterMap.keySet, resourceMap)
                              }
                          }
                      } else {
                        populateVariableValues(fhirClientPartition, fhirPathEvaluator, dataPreparationRequest.featureset, eligiblePatientURIs, Option.empty)
                          .map { resourceMap: Map[String, Map[String, Any]] =>
                            convertToSparkRow(dataPreparationRequest.featureset, eligiblePatientURIs, resourceMap)
                          }
                      }
                    }
                  }
                }

                try { // Wait for the whole execution to be completed on a worker node
                  Await.result(theFuture, Duration(60, TimeUnit.MINUTES))
                } catch {
                  case e: TimeoutException =>
                    val msg = s"The data preparation cannot be completed on a worker node with pageIndex:${pageIndex} within 60 minutes."
                    logger.error(msg, e)
                    throw DataPreparationException(msg, e)
                }
              }
            }
          })

          try {
            // If any Failure is found, throw the exception and stop execution
            rdd.filter(_.isFailure).map {
              case Failure(e) => {
                logger.error(e.getMessage, e)
                throw DataPreparationException(e.getMessage, e)
              }
            }

            // RDDs are lazy evaluated. In order to materialize the above statement, call an action on rdd such as foreach, collect, count etc.
            val dataRowSet = rdd.collect() // Collect the Seq[Row]s from the worker nodes
              .map(_.get) // Unwrap the Success (we know that there is no Failure since we stop execution if we find a Failure above)
              .toSeq // Covert the Array[Seq[Row]]s to Seq[Seq[Row]]s
              .flatten // Create a single Seq[Row] by merging all Seq[Row]s

            logger.debug("Data is collected from the worker nodes. And now the DataFrame will be constructed.")

            val structureSchema = DataPreparationUtil.generateSchema(dataPreparationRequest.featureset)

            val dataFrame = sparkSession.createDataFrame(
              sparkSession.sparkContext.parallelize(dataRowSet), // After collecting the data from the worker nodes, parallelize it again
              structureSchema)

            // Save the dataFrame which includes the prepared data into ppddm-store/datasets/:dataset_id
            saveDataFrame(dataPreparationRequest.dataset_id, dataFrame)

            val variables = dataPreparationRequest.featureset.variables
            if (variables.isEmpty) {
              logger.warn("This should not have HAPPENED!!! There are no variables in the data preparation request.")
              throw DataPreparationException("There are no variables in the data preparation request.")
            } else {
              val agentDataStatistics: AgentDataStatistics = StatisticsController.calculateStatistics(dataFrame, variables)
              val dataPreparationResult = DataPreparationResult(dataPreparationRequest.dataset_id, dataPreparationRequest.agent, agentDataStatistics, None)
              saveDataPreparationResult(dataPreparationResult)
            }

            // We print the schema and the data only for debugging purposes. Will be removed in the future.
            dataFrame.printSchema()
            dataFrame.show(false)

            Done
          } catch {
            case d: DataPreparationException =>
              // Save DataPreparationResult with DataPreparationException
              val dataPreparationResult = DataPreparationResult(dataPreparationRequest.dataset_id, dataPreparationRequest.agent,
                AgentDataStatistics(0L, Seq.empty[VariableStatistics]), Some(d.getMessage))
              saveDataPreparationResult(dataPreparationResult)
              throw d
            case e: Exception =>
              val msg = s"An unexpected error occurred while preparing data"
              logger.error(msg)
              logger.error(msg, e)

              // Save DataPreparationResult with Exception
              val dataPreparationException = DataPreparationException(msg, e)
              val dataPreparationResult = DataPreparationResult(dataPreparationRequest.dataset_id, dataPreparationRequest.agent,
                AgentDataStatistics(0L, Seq.empty[VariableStatistics]), Some(dataPreparationException.getMessage))
              saveDataPreparationResult(dataPreparationResult)
              throw dataPreparationException
          }
        } else {
          logger.info("There are no patients for the given eligibility criteria: {}", dataPreparationRequest.eligibility_criteria)

          logger.debug("An empty DataPreparationResult will be saved into the DataStore for Dataset:{}", dataPreparationRequest.dataset_id)
          val dataPreparationResult: DataPreparationResult = DataPreparationResult(dataPreparationRequest.dataset_id,
            dataPreparationRequest.agent,
            AgentDataStatistics(0L, Seq.empty[VariableStatistics]), None)

          saveDataPreparationResult(dataPreparationResult)

          Done
        }
      }
    }
  }

  private def saveDataFrame(dataset_id: String, dataFrame: DataFrame): Unit = {
    try {
      // Save the dataFrame which includes the prepared data into ppddm-store/datasets/:dataset_id
      AgentDataStoreManager.saveDataFrame(AgentDataStoreManager.getDatasetPath(dataset_id), dataFrame)
      logger.info(s"Prepared data has been successfully saved with id: ${dataset_id}")
    }
    catch {
      case e: Exception =>
        try {
          // Try saving once more
          AgentDataStoreManager.saveDataFrame(AgentDataStoreManager.getDatasetPath(dataset_id), dataFrame)
          logger.info(s"Prepared data has been successfully saved with id: ${dataset_id}")
        }
        catch {
          case e: Exception =>
            val msg = s"Cannot save the Dataframe of the prepared data with id: ${dataset_id} due to following error:"
            logger.error(msg)
            logger.error(e.getMessage)
            throw DataPreparationException(msg, e)
        }
    }
  }

  private def saveDataPreparationResult(dataPreparationResult: DataPreparationResult): Unit = {
    try {
      AgentDataStoreManager.saveDataFrame(
        AgentDataStoreManager.getStatisticsPath(dataPreparationResult.dataset_id),
        Seq(dataPreparationResult.toJson).toDF())
      logger.info("DataPreparationResult containing the calculated statistics have been successfully saved.")
    } catch {
      case e: Exception =>
        try {
          // Try saving once more
          AgentDataStoreManager.saveDataFrame(
            AgentDataStoreManager.getStatisticsPath(dataPreparationResult.dataset_id),
            Seq(dataPreparationResult.toJson).toDF())
          logger.info("DataPreparationResult containing the calculated statistics have been successfully saved.")
        } catch {
          case e: Exception =>
            val msg = s"Cannot save DataPreparationResult with id: ${dataPreparationResult.dataset_id} due to following error:"
            logger.error(msg)
            logger.error(e.getMessage)
            throw DataPreparationException(msg, e)
        }
    }
  }

  /**
   * Given the result set of values in the form of a Map, converts them to a sequence of Spark Row
   *
   * @param featureset  The Featureset which contains the Variable definitions
   * @param patientURIs The URIs (/Patient/12323-2312-231) of the patients
   * @param resourceMap The result set in the form returned by #populateVariableValues
   * @return A Seq of Row
   */
  private def convertToSparkRow(featureset: Featureset, patientURIs: Set[String], resourceMap: Map[String, Map[String, Any]]): Seq[Row] = {
    // Now create RDD Rows. Consider that we are trying to create data in tabular format here.
    // Each Row will correspond to a patient, hence do the same operation for each patient.
    patientURIs
      .toSeq // Covert to sequence to achieve a Row[Seq] in th end. At this time, we are sure that we do not have duplicates.
      .map(patientURI => { // For each patient
        val rowValues = featureset.variables.map(variable => { // For each variable
          val resourceForVariable = resourceMap(variable.name) // For the variable, get values for all patients
          resourceForVariable.get(patientURI) // Find the corresponding patient and write the value to the corresponding cell.
        }).filter(_.isDefined) // Keep values which exist for the given patients
          .map(_.get) // Convert the map to Seq[Any]
        // Create a Row from the values extracted for a single patient. Add the patientID to the beginning.
        // Note that the values in the sequence are ordered in the order of the variable sequence in the featureset
        Row.fromSeq(Seq(patientURI) ++ rowValues)
        //s"${patientURI},${rowValues.mkString(",")}"// first column will be Patient ID. Concatenate it with patient ID and create the output Row
      })
  }

  /**
   * For each variable, executes #fetchValuesOfVariable and then merges the results to end up with a combined result set.
   *
   * @param fhirClient        The FHIRClient
   * @param fhirPathEvaluator The FhirPathEvaluator
   * @param featureset        The Featureset which contains the Variable definitions
   * @param resourceURIs       The URIs (/Patient/12323-2312-231) of the patients
   * @return A Future of the Map in the following form:
   *
   *         Map(Smoking status -> Map(Patient/5dea8608a8273d7cac52005d44a59360 -> 1, Patient/7376b017a75b043a40f0f4ed654852f0 -> 0, Patient/4e6e2d0e0439cbaf75c7f914a5e111d3 -> 0,
   *         Patient/04e5829775e37d63b7a236d9f591e1cb -> 0, Patient/16ed073856be0118031ac67309198420 -> 0, Patient/6602b46629ae17ab1145aaa0fc1f625e -> 0,
   *         Patient/1f02a91bb9343781e02c7f112d7b791d -> 1, Patient/9cd69c88a9526c8a5acabe24504ae497 -> 1, Patient/50605e9eb98ebc79321a9f6b5a8fa0cf -> 1,
   *         Patient/10e5dfbc9116508271574a10beec20a7 -> 0),
   *         Number of prescribed drugs -> Map(Patient/5dea8608a8273d7cac52005d44a59360 -> 0, Patient/7376b017a75b043a40f0f4ed654852f0 -> 0, Patient/4e6e2d0e0439cbaf75c7f914a5e111d3 -> 0,
   *         Patient/04e5829775e37d63b7a236d9f591e1cb -> 0, Patient/16ed073856be0118031ac67309198420 -> 0, Patient/6602b46629ae17ab1145aaa0fc1f625e -> 0,
   *         Patient/1f02a91bb9343781e02c7f112d7b791d -> 0, Patient/9cd69c88a9526c8a5acabe24504ae497 -> 0, Patient/50605e9eb98ebc79321a9f6b5a8fa0cf -> 0,
   *         Patient/10e5dfbc9116508271574a10beec20a7 -> 0))
   *
   */
  private def populateVariableValues(fhirClient: FHIRClient, fhirPathEvaluator: FhirPathEvaluator,
                                     featureset: Featureset, resourceURIs: Set[String],
                                     encounterMap: Option[Map[String, EncounterBasedItem]]): Future[Map[String, Map[String, Any]]] = {
    val queryFutures = featureset.variables
      .map(fetchValuesOfVariable(fhirClient, fhirPathEvaluator, resourceURIs, encounterMap, _)) // For each variable, fetch the values for each patient

    Future.sequence(queryFutures) // Join the parallel Futures
      .map { queryResults =>
        queryResults.reduceLeft((a, b) => a ++ b) // Add the maps together so that each entry holds the data for one variable
      }
  }

  /**
   * Find the eligible patients.
   *
   * @param fhirClient           The FHIRClient to be used for FHIR communication
   * @param fhirPathEvaluator    The FHIRPath evaluator to be used for evaluating the FHIRPath expressions
   * @param eligibility_criteria The sequence of EligibilityCriterion to construct the associated FHIR search queries
   * @param patientQuery         The FHIRQuery associated with the Patient resources.
   * @param pageIndex            The index of the spark worker node which calls this function.
   * @return The IDs of the eligible patients.
   */
  private def findEligiblePatients(fhirClient: FHIRClient, fhirPathEvaluator: FhirPathEvaluator,
                                   eligibility_criteria: Seq[EligibilityCriterion], patientQuery: FHIRQuery,
                                   pageIndex: Int): Future[Set[String]] = {
    // Fetch the Patient resources from the FHIR Repository and collect their IDs
    patientQuery.getResources(fhirClient, batchSize, pageIndex) flatMap { resources =>

      // If fhir_path is non-empty, filter resulting patients which satisfy the FHIR path expression
      val patientURIs: Set[String] = if (patientQuery.getFHIRPath().nonEmpty && resources.nonEmpty) {
        val fhirPathExpression = patientQuery.getFHIRPath().get

        if (fhirPathExpression.startsWith(FHIRPathExpressionPrefix.AGGREGATION)) {
          // If FHIRPath expression starts with 'FHIRPathExpressionPrefix.AGGREGATION'
          val expr: String = fhirPathExpression.substring(FHIRPathExpressionPrefix.AGGREGATION.length) // Extract the actual FHIRPath expression
          val result = Try(fhirPathEvaluator.evaluateString(expr, JArray(resources.toList))).getOrElse(Seq.empty) // Evaluate path with aggregation. It should return a list of PatientIDs.
          result
            .map(pid => s"Patient/$pid") // Prepend the Patient keyword to each pid
            .toSet // Convert to set
        } else if (fhirPathExpression.startsWith(FHIRPathExpressionPrefix.SATISFY)) {
          // If FHIRPath expression starts with 'FHIRPathExpressionPrefix.SATISFY'
          resources
            .filter(item => Try(fhirPathEvaluator.satisfies(fhirPathExpression.substring(FHIRPathExpressionPrefix.SATISFY.length), item)).getOrElse(false)) // Filter the resources by whether they satisfy the path expression.
            .map(r => (r \ "id").asInstanceOf[JString].values) // Collect the patientIDs
            .map(pid => s"Patient/$pid") // Prepend the Patient keyword to each pid
            .toSet // Convert to set
        } else {
          // Invalid FHIRPath expression
          logger.error("Invalid fhir_path expression prefix in eligibility_criteria: {}", fhirPathExpression)
          throw DataPreparationException(s"Invalid fhir_path expression prefix in eligibility_criteria: $fhirPathExpression")
        }
      } else {
        resources
          .map(p => (p \ "id").asInstanceOf[JString].values) // extract the IDs from the JObject of Patient
          .map(pid => s"Patient/$pid") // Prepend the Patient keyword to each pid
          .toSet // Convert to set
      }

      if (patientURIs.isEmpty) {
        // There are no eligible patients in this partition. We do not need to execute the remaining criteria on other resources
        logger.debug(s"There are no eligible patients in this partition at page index ${pageIndex}.") // Log the result
        Future {
          Set.empty[String]
        }
      } else {
        logger.debug(s"Finding eligible patients at page index ${pageIndex}")

        // Fetch the remaining resources (in parallel within worker node) indicated within the eligibilityCriteria
        val criteriaOtherThanPatientResource = eligibility_criteria.filterNot(_.fhir_query.startsWith("/Patient")).filterNot(_.fhir_query.startsWith("/Encounter"))
        if (criteriaOtherThanPatientResource.isEmpty) {
          // There is only one criterion on the Patient resource, hence we need to use the already retrieved patientsIDs as the eligible patients
          logger.debug(s"${patientURIs.size} eligible patients are found.") // Log the result
          Future {
            patientURIs
          }
        } else {
          // There are other resources in addition to the Patient resource query. Hence, they need to be executed to narrow down the eligible patients
          val queryFutures = criteriaOtherThanPatientResource.map(findPatientIDsOfCriterion(fhirClient, fhirPathEvaluator, patientURIs, _))

          Future.sequence(queryFutures) // Join the parallel Futures
            .map { queryResults =>
              val result = queryResults.reduceLeft((a, b) => a.intersect(b)) // Create a final list by intersecting the resulting lists coming from criterionOtherThanPatientResource
              logger.debug(s"${result.size} eligible patients are found at page index ${pageIndex}.") // Log the result
              result
            }
        }
      }
    }
  }

  /**
   * Find the eligible encounters, with given patient references and satisfying eligible criteria
   *
   * @param fhirClient          The FHIRClient to be used for FHIR communication
   * @param patientURIs         The eligible Patient references
   * @param encounterCriterion   The eligibility criterion for Encounters
   * @return A future in form of a map (encounterReference -> (subjectReference, encounterPeriodStart, encounterPeriodEnd, encounterType)):
   *         (Encounter/5dea8608a8273d7cac52005d44a59360 -> (Patient/7cd82663ac897d2399147b706ac5c37c, 2010-05-11T00:00:00.000Z, 2010-06-11T00:00:00.000Z, planned),
   *         Encounter/991e52abaae14cda69c0a3e80eaf5f4a -> (Patient/e58c39736ab87b03b72e1b0d449222dc, 2010-04-22T00:00:00.000Z, 2010-05-12T00:00:00.000Z, unplanned))
   */
  private def findEligibleEncounters(fhirClient: FHIRClient, patientURIs: Set[String], encounterCriterion: EligibilityCriterion): Future[Map[String, EncounterBasedItem]] = {
    // Create fhir Query and collect all encounters of eligible patients
    val fhirQuery = QueryHandler.getResourcesOfPatientsQuery(patientURIs, encounterCriterion.fhir_query, encounterCriterion.fhir_path)
    var encounterMap: Map[String, EncounterBasedItem] = Map.empty
    fhirQuery.getResources(fhirClient, all = true) map { encounters =>
      // For each encounter, fill the encounter based items.
      // e.g. Map(Encounter/e1 -> EncounterBasedItem(encounterSubject, encounterStart, encounterEnd)).
      encounters.foreach(encounter => {
        try {
          val encounterID = (encounter \ "id").extract[String]
          encounterMap += (s"Encounter/$encounterID" -> extractEncounterBasedItem(encounter))
        } catch {
          case e: Exception =>
            logger.error(s"Error occurred while parsing the Encounter resource: $encounter. $e")
        }
      })
      if (encounterCriterion.fhir_path.nonEmpty) {
        val fhirPathExpression = encounterCriterion.fhir_path.get
        if (fhirPathExpression.startsWith(FHIRPathExpressionPrefix.VALUE_READMISSION)) {
          val day = fhirPathExpression.substring(FHIRPathExpressionPrefix.VALUE_READMISSION.length).toInt

          var extractedMap = Map[String, EncounterBasedItem]()
          encounterMap.foreach { encounter =>
            // Filter the encounters by subject
            val currentSubjectEncounters = encounterMap.filter(_._2.subject == encounter._2.subject)
            if (currentSubjectEncounters.nonEmpty) {
              // End date of the current encounter
              val currEncounterEndDate = ZonedDateTime.parse(encounter._2.periodEnd)
              // Calculate X days after from the end date of the current encounter
              val currEncounterEndDateXDaysAfter = ZonedDateTime.parse(encounter._2.periodEnd).plusDays(day)
              // If there exists an encounter between these dates, then has readmission will have value
              val hasReadmission = currentSubjectEncounters.filter {e =>
                val nextEncounterStartDate = ZonedDateTime.parse(e._2.periodStart)
                nextEncounterStartDate.isAfter(currEncounterEndDate) && nextEncounterStartDate.isBefore(currEncounterEndDateXDaysAfter) &&
                  encounter._2.encounterType.nonEmpty && encounter._2.encounterType.get == "unplanned" &&
                  e._2.encounterType.nonEmpty && e._2.encounterType.get == "planned"
              }
              if (hasReadmission.isEmpty) extractedMap += (encounter._1 -> encounter._2)
            } else {
              extractedMap += (encounter._1 -> encounter._2)
            }
          }
          extractedMap
        } else {
          encounterMap
        }
      } else {
        encounterMap
      }
    }
  }

  private def findAllEligibleEncounters(fhirClient: FHIRClient, patientURIs: Set[String], encounterCriteria: Seq[EligibilityCriterion]): Future[Map[String, EncounterBasedItem]] = {
    if (encounterCriteria.isEmpty) {
      findEligibleEncounters(fhirClient, patientURIs, EligibilityCriterion("/Encounter", Option.empty))
    } else {
      Future.sequence(encounterCriteria.map {encounterCriterion =>
        findEligibleEncounters(fhirClient, patientURIs, encounterCriterion)
      }).map { mapsToBeIntersected =>
        mapsToBeIntersected.reduceLeft((m1, m2) => m1.keySet.intersect(m2.keySet).map(k => k -> m1(k)).toMap)
      }
    }
  }

  /**
   * Get the subject, periodStart and periodEnd values from the given encounter resource
   *
   * @param encounter   The encounter resource to be extracted
   * @return - EncounterBasedItem
   */
  private def extractEncounterBasedItem(encounter: JObject): EncounterBasedItem = {
    val encounterSubject = (encounter \ "subject" \ "reference").extract[String]
    val encounterPeriodStart = Try((encounter \ "period" \ "start").extract[String]).getOrElse(ZonedDateTime.now().toString)
    val encounterPeriodEnd = Try((encounter \ "period" \ "end").extract[String]).getOrElse(ZonedDateTime.now().toString)
    val encounterType = Try((((encounter \ "type").extract[JArray].arr.head \ "coding").extract[JArray].arr.head \ "code").extract[String]).toOption
    EncounterBasedItem(encounterSubject, encounterPeriodStart, encounterPeriodEnd, encounterType)
  }

  /**
   * Given a list of patient URIs, find the resources of those patients according to the eligibilityCriteria defined for each resource
   * and then collect the subjects (patient IDs) of the resulting resources.
   * This will only retrieve the resources of the given patients. And then those patientIDs will be returned by this function.
   *
   * @param fhirClient
   * @param fhirPathEvaluator
   * @param patientURIs
   * @param eligibilityCriteria
   * @return
   */
  private def findPatientIDsOfCriterion(fhirClient: FHIRClient, fhirPathEvaluator: FhirPathEvaluator, patientURIs: Set[String],
                                        eligibilityCriteria: EligibilityCriterion): Future[Set[String]] = {

    val fhirQuery = QueryHandler.getResourcesOfPatientsQuery(patientURIs, eligibilityCriteria.fhir_query, eligibilityCriteria.fhir_path)

    fhirQuery.getResources(fhirClient, all = true) map { resources =>
      if (eligibilityCriteria.fhir_path.nonEmpty && resources.nonEmpty) {
        val fhirPathExpression = eligibilityCriteria.fhir_path.get

        if (fhirPathExpression.startsWith(FHIRPathExpressionPrefix.AGGREGATION)) {
          // If FHIRPath expression starts with 'FHIRPathExpressionPrefix.AGGREGATION'
          val expr: String = fhirPathExpression.substring(FHIRPathExpressionPrefix.AGGREGATION.length) // Extract the actual FHIRPath expression
          val result = Try(fhirPathEvaluator.evaluateString(expr, JArray(resources.toList))).getOrElse(Seq.empty) // Evaluate path with aggregation. It should return a list of PatientIDs.
          result.toSet // Convert to set
        } else if (fhirPathExpression.startsWith(FHIRPathExpressionPrefix.SATISFY)) {
          // If FHIRPath expression starts with 'FHIRPathExpressionPrefix.SATISFY'
          resources
            .filter(item => Try(fhirPathEvaluator.satisfies(fhirPathExpression.substring(FHIRPathExpressionPrefix.SATISFY.length), item)).getOrElse(false)) // Filter the resources by whether they satisfy the path expression.
            .map(r => (r \ "subject" \ "reference").asInstanceOf[JString].values) // Collect the patientIDs
            .toSet // Convert to set
        } else {
          // Invalid FHIRPath expression
          logger.error("Invalid fhir_path expression prefix in eligibility_criteria: {}", fhirPathExpression)
          throw DataPreparationException(s"Invalid fhir_path expression prefix in eligibility_criteria: $fhirPathExpression")
        }
      } else {
        resources
          .map(r => (r \ "subject" \ "reference").asInstanceOf[JString].values) // Collect the patientIDs
          .toSet // Convert to set
      }
    }
  }

  /**
   * Executes the fhir_query and fhir_path for the given variable on the given patientURIs and returns the values of the variable for each patient.
   * This dataset corresponds to the column defined for the given variable, filled with the values of that variable for the given patients.
   *
   * @param fhirClient
   * @param resourceURIs
   * @param variable
   * @return returns a future in form of the following Map where the Map has only one entry:
   *         Map(Smoking status -> Map(Patient/5dea8608a8273d7cac52005d44a59360 -> 1, Patient/7376b017a75b043a40f0f4ed654852f0 -> 0, Patient/4e6e2d0e0439cbaf75c7f914a5e111d3 -> 0,
   *         Patient/04e5829775e37d63b7a236d9f591e1cb -> 0, Patient/16ed073856be0118031ac67309198420 -> 0, Patient/6602b46629ae17ab1145aaa0fc1f625e -> 0,
   *         Patient/1f02a91bb9343781e02c7f112d7b791d -> 1, Patient/9cd69c88a9526c8a5acabe24504ae497 -> 1, Patient/50605e9eb98ebc79321a9f6b5a8fa0cf -> 1,
   *         Patient/10e5dfbc9116508271574a10beec20a7 -> 0))
   */
  private def fetchValuesOfVariable(fhirClient: FHIRClient, fhirPathEvaluator: FhirPathEvaluator, resourceURIs: Set[String],
                                    encounterMap: Option[Map[String, EncounterBasedItem]],variable: Variable): Future[Map[String, Map[String, Any]]] = {
    // FHIR Query will differ depending on whether the data is being prepared as encounter-based or not.
    // If the encounterMap is defined, we understand that it is encounter-based.
    val fhirQuery: FHIRQuery =
      if (encounterMap.isEmpty)
        QueryHandler.getResourcesOfPatientsQuery(resourceURIs, variable.fhir_query, Some(variable.fhir_path))
      else
        QueryHandler.getResourcesOfEncountersQuery(resourceURIs, encounterMap.get, variable.fhir_query, Some(variable.fhir_path))

    fhirQuery.getResources(fhirClient, all = true) map { resources =>
      if (variable.fhir_path.startsWith(FHIRPathExpressionPrefix.AGGREGATION)) {
        // If FHIRPath expression starts with 'FHIRPathExpressionPrefix.AGGREGATION'
        evaluateAggrPath4FeatureSet(fhirPathEvaluator, resources, resourceURIs, variable, encounterMap)
      } else if (variable.fhir_path.startsWith(FHIRPathExpressionPrefix.VALUE_READMISSION)) {
        // If FHIRPath expression starts with 'FHIRPathExpressionPrefix.VALUE_READMISSION'
        val encounterResources = resources.filter {resource =>
          (resource \ "resourceType").extract[String] == "Encounter"
        }
        evaluateReadmissionValue(encounterMap.get, encounterResources, variable)
      } else if (variable.fhir_path.startsWith(FHIRPathExpressionPrefix.VALUE_HOSPITALIZATION)) {
        // If FHIRPath expression starts with 'FHIRPathExpressionPrefix.VALUE_HOSPITALIZATION'
        val encounterResources = resources.filter {resource =>
          (resource \ "resourceType").extract[String] == "Encounter"
        }
        evaluateHospitalizationValue(encounterMap.get, encounterResources, variable)
      } else if (variable.fhir_path.startsWith(FHIRPathExpressionPrefix.VALUE)) {
        // If FHIRPath expression starts with 'FHIRPathExpressionPrefix.VALUE'
        evaluateValuePath4FeatureSet(fhirPathEvaluator, resources, resourceURIs, variable, encounterMap)
      } else {
        // Invalid FHIRPath expression
        logger.error("Invalid FHIRPath expression prefix in featureset variables: {}", variable.fhir_path)
        throw DataPreparationException(s"Invalid FHIRPath expression prefix in featureset variables: ${variable.fhir_path}")
      }
    }
  }

  /**
   * Returns True if there is a variable with Encounter resource type
   *
   * @param featureset
   * @return
   */
  private def hasEncounterTypeVariable(featureset: Featureset): Boolean = {
    featureset.variables.exists(variable => variable.fhir_path.contains("hospitalization") || variable.fhir_path.contains("readmission"))
  }

  /**
   * Evaluates the readmission scores one by one for each encounter in the encounterMap.
   * It calculates the period by subtracting the next_encounter_startDate from the current_encounter_endDate
   *
   * @param encounterMap  The encounter map in the form of (Encounter/e1 -> EncounterBasedItem(Patient/p1, 2020-01-10, 2020-02-10), ...)
   * @param variable      The Variables
   * @return returns a map
   *         Map(readmitted_in_30_days -> Map(Encounter/e1 -> 1.0, Encounter/e2 -> 0.0, ...))
   */
  def evaluateReadmissionValue(encounterMap: Map[String, EncounterBasedItem], encounters: Seq[JObject], variable: Variable): Map[String, Map[String, Any]] = {
    val initialValuesForAllResources: Map[String, Any] = encounterMap.keySet.map((_ -> 0.toDouble)).toMap
    // Get the day information from the fhir_path expression
    val day = variable.fhir_path.substring(FHIRPathExpressionPrefix.VALUE_READMISSION.length).toInt

    if (encounters.nonEmpty) {
      var encounterMap: Map[String, EncounterBasedItem] = Map.empty
      // For each encounter, fill the encounter based items.
      // e.g. Map(Encounter/e1 -> EncounterBasedItem(encounterSubject, encounterStart, encounterEnd)).
      encounters.foreach(encounter => {
        try {
          val encounterID = (encounter \ "id").extract[String]
          encounterMap += (s"Encounter/$encounterID" -> extractEncounterBasedItem(encounter))
        } catch {
          case e: Exception =>
            logger.error(s"Error occurred while parsing the Encounter resource: $encounter. $e")
        }
      })
      val extractedMap = encounterMap.map { encounter =>
        // Filter the encounters by subject
        val currentSubjectEncounters = encounterMap.filter(_._2.subject == encounter._2.subject)
        if (currentSubjectEncounters.nonEmpty) {
          // End date of the current encounter
          val currEncounterEndDate = ZonedDateTime.parse(encounter._2.periodEnd)
          // Calculate X days after from the end date of the current encounter
          val currEncounterEndDateXDaysAfter = ZonedDateTime.parse(encounter._2.periodEnd).plusDays(day)
          // If there exists an encounter between these dates, then has readmission will have value
          val hasReadmission = currentSubjectEncounters.filter {e =>
            val nextEncounterStartDate = ZonedDateTime.parse(e._2.periodStart)
            nextEncounterStartDate.isAfter(currEncounterEndDate) &&
              nextEncounterStartDate.isBefore(currEncounterEndDateXDaysAfter) &&
              e._2.encounterType.nonEmpty && e._2.encounterType.get == "unplanned"
          }
          if (hasReadmission.nonEmpty) encounter._1 -> 1.toDouble
          else encounter._1 -> 0.toDouble
        } else {
          // If no encounter is found for the subject, fill it with 0.
          encounter._1 -> 0.toDouble
        }
      }
      Map(variable.name -> (initialValuesForAllResources ++ extractedMap))
    } else {
      Map(variable.name -> initialValuesForAllResources)
    }
  }

  /**
   * Counts the hospitalizations in the last 12 months. For each encounter, looks for another encounter
   * whose periodStart date is bigger than xMonthsBefore from today. And also periodEnd date is smaller than
   * current encounter periodStart date.
   *
   * @param encounters  The encounter resources
   * @param variable    The Variables
   * @return returns a map
   *         Map(hospitalization_12_months -> Map(Encounter/e1 -> 3.0, Encounter/e2 -> 1.0, Encounter/e3 -> 0.0, ...))
   */
  def evaluateHospitalizationValue(encounterMap: Map[String, EncounterBasedItem], encounters: Seq[JObject], variable: Variable): Map[String, Map[String, Any]] = {
    val initialValuesForAllResources: Map[String, Any] = encounterMap.keySet.map((_ -> 0.toDouble)).toMap

    if (encounters.nonEmpty) {
      var encounterMap: Map[String, EncounterBasedItem] = Map.empty
      // For each encounter, fill the encounter based items.
      // e.g. Map(Encounter/e1 -> EncounterBasedItem(encounterSubject, encounterStart, encounterEnd)).
      encounters.foreach(encounter => {
        try {
          val encounterID = (encounter \ "id").extract[String]
          encounterMap += (s"Encounter/$encounterID" -> extractEncounterBasedItem(encounter))
        } catch {
          case e: Exception =>
            logger.error(s"Error occurred while parsing the Encounter resource: $encounter. $e")
        }
      })
      // Get the month information from the fhir_path expression
      val month = variable.fhir_path.substring(FHIRPathExpressionPrefix.VALUE_HOSPITALIZATION.length).toInt
      val xMonthsBefore = ZonedDateTime.now().minusMonths(month)
      val extractedMap = encounterMap.map { encounter =>
        // Filter the encounters by subject
        val currentSubjectEncounters = encounterMap.filter(_._2.subject == encounter._2.subject)
        if (currentSubjectEncounters.nonEmpty) {
          // Start date of the current encounter
          val currEncounterStartDate = ZonedDateTime.parse(encounter._2.periodStart)
          // If there is an encounter between these dates
          val hospitalizationNum = currentSubjectEncounters.filter {e =>
            val prevEncounterStartDate = ZonedDateTime.parse(e._2.periodStart)
            val prevEncounterEndDate = ZonedDateTime.parse(e._2.periodEnd)
            (!prevEncounterStartDate.isEqual(prevEncounterEndDate)) &&
              (prevEncounterEndDate.isEqual(currEncounterStartDate) || prevEncounterEndDate.isBefore(currEncounterStartDate)) &&
              (prevEncounterStartDate.isEqual(xMonthsBefore) || prevEncounterStartDate.isAfter(xMonthsBefore))
          }
          if (hospitalizationNum.nonEmpty) encounter._1 -> hospitalizationNum.size.toDouble
          else encounter._1 -> 0.toDouble
        } else {
          // If no encounter is found for the subject, fill it with 0.
          encounter._1 -> 0.toDouble
        }
      }
      Map(variable.name -> (initialValuesForAllResources ++ extractedMap))
    } else {
      Map(variable.name -> initialValuesForAllResources)
    }
  }

  /**
   * Evaluates the fhir_path expression with the aggregation prefix (FHIRPathExpressionPrefix.AGGREGATION) for the given Variable.
   * It is assumed that all aggregation functions will result in a numeric value (such as count/sum/average/max) and the
   * data type is configured as a Double for all numeric values (even if it is an integer, it will be casted)
   *
   * @param fhirPathEvaluator
   * @param resources
   * @param variable
   * @return returns a map in the following form:
   *         Map(Prescribed Drugs -> Map(Patient/5dea8608a8273d7cac52005d44a59360 -> 6, ...))
   */
  def evaluateAggrPath4FeatureSet(fhirPathEvaluator: FhirPathEvaluator, resources: Seq[JObject], resourceURIs: Set[String],
                                  variable: Variable, encounterMap: Option[Map[String, EncounterBasedItem]]): Map[String, Map[String, Any]] = {

    val isCategoricalType = variable.variable_data_type == VariableDataType.CATEGORICAL
    // If variable data type is Categorical set null otherwise 0
    val initialValue = if (isCategoricalType) null else 0.toDouble
    var initialValuesForAllResources: Map[String, Any] = resourceURIs.map((_ -> initialValue)).toMap

    if (resources.nonEmpty) {
      if (variable.fhir_path.startsWith(FHIRPathExpressionPrefix.AGGREGATION_EXISTENCE)) {
        // Remove FHIR Path expression prefix 'FHIRPathExpressionPrefix.AGGREGATION_EXISTENCE'
        val fhirPathExpression: String = variable.fhir_path.substring(FHIRPathExpressionPrefix.AGGREGATION_EXISTENCE.length)
        // Evaluate FHIR Path on the resource list
        val result = fhirPathEvaluator.evaluateString(fhirPathExpression, JArray(resources.toList))
        val extractedValues = result.map { item =>
          item -> 1.toDouble // Fill with 1 for existing resources
        }.toMap

        Map(variable.name -> (initialValuesForAllResources ++ extractedValues))
      } else {
        // Remove FHIR Path expression prefix 'FHIRPathExpressionPrefix.AGGREGATION'
        val fhirPathExpression: String = variable.fhir_path.substring(FHIRPathExpressionPrefix.AGGREGATION.length)
        // Evaluate FHIR Path on the resource list
        val result = fhirPathEvaluator.evaluate(fhirPathExpression, JArray(resources.toList))
        val extractedValues = result.map { item =>

          /**
           * complexResult is a List[(a,b)] in which we are sure that there are two tuples as follows:
           * List[(bucket -> JString(Patient/p1), (agg -> JLong(2)))
           * OR
           * List[(bucket -> JString(Encounter/e1), (agg -> JLong(2)))
           */
          val complexResult = item.asInstanceOf[FhirPathComplex] // retrieve as a complex result
            .json.obj // Access to the List[(String, JValue)]

          import ppddm.core.util.JsonFormatter._

          val patientID: String = complexResult.filter(_._1 == "bucket").head._2.extract[String]
          val aggrResult: Double = complexResult.filter(_._1 == "agg").head._2.extract[Double]
          patientID -> aggrResult // Patient ID -> count
        }.toMap

        // If encounterMap is defined (means that data is being prepared Encounter-based), fill the values of Encounters in the map
        if (encounterMap.isDefined) {
          extractedValues.foreach { keyValuePair =>
            val matchedEncounters = encounterMap.get.filter(e => {
              if (keyValuePair._1.startsWith("Patient")) e._2.subject == keyValuePair._1 // If the reference is a Patient resource
              else e._1 == keyValuePair._1 // If the reference is an Encounter resource
            })
            if (matchedEncounters.nonEmpty) {
              matchedEncounters.keys.foreach { encounterRef =>
                initialValuesForAllResources += (encounterRef -> keyValuePair._2)
              }
            }
          }
          Map(variable.name -> initialValuesForAllResources)
        } else {
          Map(variable.name -> (initialValuesForAllResources ++ extractedValues))
        }
      }
    } else {
      Map(variable.name -> initialValuesForAllResources)
    }
  }

  /**
   * Evaluates the fhir_path expression with the value prefix (FHIRPathExpressionPrefix.VALUE) for the given Variable.
   * Evaluates it on resources one by one and extracts the value according to the FHIRPath expression.
   *
   * @param fhirPathEvaluator
   * @param resources
   * @param variable
   * @return returns a map in the following form:
   *         Map(Gender -> Map(Patient/5dea8608a8273d7cac52005d44a59360 -> male, ...))
   *         OR
   *         Map(Gender -> Map(Encounter/2dea8608a8273d7cac52005d44a59360 -> male, ...))
   */
  def evaluateValuePath4FeatureSet(fhirPathEvaluator: FhirPathEvaluator, resources: Seq[JObject], resourceURIs: Set[String],
                                   variable: Variable, encounterMap: Option[Map[String, EncounterBasedItem]]): Map[String, Map[String, Any]] = {

    val isCategoricalType = variable.variable_data_type == VariableDataType.CATEGORICAL
    // If variable data type is Categorical set null otherwise 0
    val initialValue = if (isCategoricalType) null else 0.toDouble
    var initialValuesForAllResources: Map[String, Any] = resourceURIs.map((_ -> initialValue)).toMap

    // Remove FHIR Path expression prefix 'FHIRPathExpressionPrefix.VALUE'
    val fhirPathExpression: String = variable.fhir_path.substring(FHIRPathExpressionPrefix.VALUE.length)

    val isPatient = variable.fhir_query.startsWith("/Patient")
    val lookingForExistence: Boolean = fhirPathExpression.startsWith("exists")

    val extractedValues = resources
      .map { resource => // For each resulting resource
        /**
         * Set the value null if the variable data type is categorical or the fhir_path is not looking for existence.
         * e.g. Observation.valueQuantity.value field is numeric and fhir_path is not looking for existence,
         * and if its fhir_path evaluation result is None, the value should be null.
         * Setting the value 1 means that the variable is looking for the existence, and its default value is 1 if it exists among resources.
         */
        var value = if (isCategoricalType || !lookingForExistence) null else 1.toDouble
        if (!lookingForExistence) {
          val resultOption = Try(fhirPathEvaluator.evaluate(fhirPathExpression, resource).headOption).getOrElse(None)
          resultOption.foreach { result =>
            // If we will get the value from the FHIRPath expression, then take it from the evaluation result
            value = result match {
              case FhirPathString(s) => s
              case FhirPathNumber(v) => v.toDouble
              case FhirPathDateTime(dt) => dt.toString
              case _ =>
                val msg = s"Unsupported FHIRPath result type: ${result}"
                logger.error(msg)
                throw DataPreparationException(msg)
            }
          }
        }
        /**
         * If it is Patient resource get the id from Patient.id. Otherwise, get it from [Resource].subject.reference
         * The resulting map will be in the form of: Map(Patient_id or Encounter_id -> (value, Option(date)))
         * e.g. (Patient/p1 -> (10, Option(2020-01-12)), ...)
         *      OR depending on Encounter-based data preparation:
         *      (Encounter/e1 -> (10, Option(2020-01-12)))
         */
        if (isPatient) {
          "Patient/" + (resource \ "id").asInstanceOf[JString].values -> (value, Option.empty)
        } else if (variable.fhir_query.startsWith("/Encounter")) {
          if (encounterMap.isDefined) "Encounter/" + (resource \ "id").asInstanceOf[JString].values -> (value, Option.empty)
          else (resource \ "subject" \ "reference").asInstanceOf[JString].values -> (value, Option.empty)
        } else {
          // Reference parameter may change depending on whether the data is being prepared as encounter-based or not.
          // It will be either subject(Patient) or encounter-context(Encounter).
          val referenceParameter = if (encounterMap.isDefined && !lookingForExistence) {
            if (variable.fhir_query.startsWith("/MedicationStatement")) "context" else "encounter"
          } else "subject"
          // Date parameter may change depending on the resource type.
          var dateParameter = ""
          if (variable.fhir_query.startsWith("/Condition")) {
            dateParameter = "onsetDateTime"
          } else if (variable.fhir_query.startsWith("/Observation") || variable.fhir_query.startsWith("/MedicationStatement")) {
            dateParameter = "effectiveDateTime"
          }
          val date: Option[String] = Try((resource \ dateParameter).asInstanceOf[JString].values).toOption
          (resource \ referenceParameter \ "reference").asInstanceOf[JString].values -> (value, date)
        }
      }
      .groupBy(_._1) // Group by the resource reference
      .map(_._2.head) // Take the first value

    // If encounterMap is defined (means that data is being prepared Encounter-based), fill the values of Encounters in the map
    if (encounterMap.isDefined) {
      extractedValues.foreach { keyValuePair =>
        val dateOfResource = if (keyValuePair._2._2.isDefined) Option(ZonedDateTime.parse(keyValuePair._2._2.get)) else Option.empty
        val matchedEncounters = encounterMap.get.filter(e => {
          if (keyValuePair._1.startsWith("Patient")) e._2.subject == keyValuePair._1 // If the reference is a Patient resource
          else e._1 == keyValuePair._1 // If the reference is an Encounter resource
        })
        if (matchedEncounters.nonEmpty) {
          matchedEncounters.foreach { encounter =>
            val encounterEndDate = ZonedDateTime.parse(encounter._2.periodEnd)
            // If the featureset variable is looking for existence, include the date filter.
            // The date of the value must be before(equal) the end date of the encounter.
            if (lookingForExistence && dateOfResource.isDefined && (dateOfResource.get.isEqual(encounterEndDate) || dateOfResource.get.isBefore(encounterEndDate))) {
              initialValuesForAllResources += (encounter._1 -> keyValuePair._2._1)
            } else if (!lookingForExistence || dateOfResource.isEmpty) {
              initialValuesForAllResources += (encounter._1 -> keyValuePair._2._1)
            }
          }
        }
      }
      Map(variable.name -> initialValuesForAllResources)
    } else {
      // While merging into initialValues, omit the date values from extractedValues
      // The form of extractedValues: Map(Patient_id or Encounter_id -> (value, Option(date)))
      // e.g. (Encounter/e1 -> (10, Option(2020-01-12)))
      Map(variable.name -> (initialValuesForAllResources ++ extractedValues.map {value => value._1 -> value._2._1}))
    }
  }

  /**
   * Retrieves the DataPreparationResult which includes the DataSourceStatistics for the given dataset_id
   *
   * @param dataset_id
   * @return
   */
  def getDataSourceStatistics(dataset_id: String): Option[DataPreparationResult] = {
    logger.debug("DataSourceStatistics is requested for the Dataset:{}", dataset_id)
    Try(
      AgentDataStoreManager.getDataFrame(AgentDataStoreManager.getStatisticsPath(dataset_id)) map { df =>
        df // Dataframe consisting of a column named "value" that holds Json inside
          .head() // Get the Array[Row]
          .getString(0) // Get Json String
          .extract[DataPreparationResult]
      }).getOrElse(None) // Returns None if an error occurs within the Try block
  }

  /**
   * Deletes Dataset and all its persisted data.
   *
   * @param dataset_id The unique identifier of the Dataset to be deleted.
   * @return
   */
  def deleteData(dataset_id: String): Option[Done] = {
    // Delete the dataset with the given dataset_id
    AgentDataStoreManager.deleteDirectory(AgentDataStoreManager.getDatasetPath(dataset_id))
    // Delete the statistics related with the given dataset_id
    val statisticsDeleted = AgentDataStoreManager.deleteDirectory(AgentDataStoreManager.getStatisticsPath(dataset_id))
    if (statisticsDeleted) {
      logger.info(s"Dataset and statistics (with id: $dataset_id) have been deleted successfully")
      Some(Done)
    } else {
      logger.info(s"Dataset and statistics (with id: $dataset_id) do not exist!")
      Option.empty[Done]
    }
  }

}
