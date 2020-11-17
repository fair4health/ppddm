package ppddm.agent.controller.prepare

import java.util.concurrent.TimeUnit

import akka.Done
import com.typesafe.scalalogging.Logger
import io.onfhir.path._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.json4s.JsonAST.JObject
import org.json4s.{JArray, JString}
import ppddm.agent.Agent
import ppddm.agent.config.AgentConfig
import ppddm.agent.exception.DataPreparationException
import ppddm.agent.spark.NodeExecutionContext._
import ppddm.agent.store.DataStoreManager
import ppddm.core.fhir.{FHIRClient, FHIRQuery}
import ppddm.core.rest.model._
import ppddm.core.util.JsonFormatter._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, TimeoutException}
import scala.util.Try


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

    if (DataStoreManager.getDataFrame(DataStoreManager.getDatasetPath(dataPreparationRequest.dataset_id)).isDefined) {
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
          val rdd: RDD[Seq[Row]] = sparkSession.sparkContext.parallelize(1 to numOfReturnPagesForQuery).mapPartitions(partitionIterator => {
            partitionIterator.map { pageIndex =>
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
                    populateVariableValues(fhirClientPartition, fhirPathEvaluator, dataPreparationRequest.featureset, eligiblePatientURIs)
                      .map { resourceMap: Map[String, Map[String, Any]] =>
                        convertToSparkRow(dataPreparationRequest.featureset, eligiblePatientURIs, resourceMap)
                      }
                  }
                }
              }

              try { // Wait for the whole execution to be completed on a worker node
                Await.result(theFuture, Duration(10, TimeUnit.MINUTES))
              } catch {
                case e: TimeoutException =>
                  val msg = s"The data preparation cannot be completed on a worker node with pageIndex:${pageIndex} within 10 minutes."
                  logger.error(msg, e)
                  throw DataPreparationException(msg, e)
              }
            }
          })

          // RDDs are lazy evaluated. In order to materialize the above statement, call an action on rdd such as foreach, collect, count etc.
          val dataRowSet = rdd.collect() // Collect the Seq[Row]s from the worker nodes
            .toSeq // Covert the Array[Seq[Row]]s to Seq[Seq[Row]]s
            .flatten // Create a single Seq[Row] by merging all Seq[Row]s

          logger.debug("Data is collected from the worker nodes. And now the DataFrame will be constructed.")

          val structureSchema = generateSchema(dataPreparationRequest.featureset)

          val dataFrame = sparkSession.createDataFrame(
            sparkSession.sparkContext.parallelize(dataRowSet), // After collecting the data from the worker nodes, parallelize it again
            structureSchema)

          try {
            // Save the dataFrame which includes the prepared data into ppddm-store/datasets/:dataset_id
            DataStoreManager.saveDataFrame(DataStoreManager.getDatasetPath(dataPreparationRequest.dataset_id), dataFrame)
            logger.info(s"Prepared data has been successfully saved with id: ${dataPreparationRequest.dataset_id}")
          }
          catch {
            case e: Exception =>
              val msg = s"Cannot save the Dataframe of the prepared data with id: ${dataPreparationRequest.dataset_id}."
              logger.error(msg, e)
              throw DataPreparationException(msg, e)
          }

          try {
            val variablesOption = dataPreparationRequest.featureset.variables
            if (variablesOption.isDefined) {
              val agentDataStatistics: AgentDataStatistics = StatisticsController.calculateStatistics(dataFrame, variablesOption.get)
              val dataPreparationResult: DataPreparationResult = DataPreparationResult(dataPreparationRequest.dataset_id, dataPreparationRequest.agent, agentDataStatistics)
              DataStoreManager.saveDataFrame(
                DataStoreManager.getStatisticsPath(dataPreparationRequest.dataset_id),
                Seq(dataPreparationResult.toJson).toDF())
              logger.info("Calculated statistics have been successfully saved.")
            } else {
              logger.warn("This should not have HAPPENED!!! There are no variables in the data preparation request.")
            }
          } catch {
            case e: Exception =>
              val msg = s"Cannot save Dataframe with id: ${dataPreparationRequest.dataset_id}."
              logger.error(msg, e)
              throw DataPreparationException(msg, e)
          }

          // We print the schema and the data only for debugging purposes. Will be removed in the future.
          dataFrame.printSchema()
          dataFrame.show(false)

          Done
        } else {
          logger.info("There are no patients for the given eligibility criteria: {}", dataPreparationRequest.eligibility_criteria)

          logger.debug("An empty DataPreparationResult will be saved into the DataStore for Dataset:{}", dataPreparationRequest.dataset_id)
          val dataPreparationResult: DataPreparationResult = DataPreparationResult(dataPreparationRequest.dataset_id,
            dataPreparationRequest.agent,
            AgentDataStatistics(0L, Seq.empty[VariableStatistics]))

          DataStoreManager.saveDataFrame(
            DataStoreManager.getStatisticsPath(dataPreparationRequest.dataset_id),
            Seq(dataPreparationResult.toJson).toDF())

          Done
        }
      }
    }
  }

  /**
   * Removes any inappropriate characters in variable names of the data preparation request and returns new one.
   *
   * @param dataPreparationRequest
   * @return
   */
  def validatePreparationRequest(dataPreparationRequest: DataPreparationRequest): DataPreparationRequest = {
    if (dataPreparationRequest.featureset.variables.isDefined) {
      if (dataPreparationRequest.eligibility_criteria.count(_.fhir_query.startsWith("/Patient")) > 1) { // Check if there are multiple Patient queries
        throw DataPreparationException(s"The Eligibility Criteria in the submitted DataPreparationRequest has multiple Patient queries. It should not be more than one.")
      } else {
        dataPreparationRequest.copy(
          featureset = dataPreparationRequest.featureset.copy( // Copy featureset with updated variables
            variables = Option( // Assign new variables
              dataPreparationRequest.featureset.variables.get // Get Variables from Option
                .map(variable => variable.copy(name = removeInvalidChars(variable.name))) // Remove invalid characters in variable.name
            )
          )
        )
      }
    } else {
      throw DataPreparationException(s"The Featureset in the submitted DataPreparationRequest does not include any Variable definitions.")
    }
  }

  /**
   * Removes invalid characters that prevent the recording and filtering of the Parquet files from being done correctly.
   *
   * @param value
   * @return
   */
  def removeInvalidChars(value: String): String = {
    value.trim.replaceAll("[\\s\\`\\*{}\\[\\]()>#\\+:\\~'%\\^&@<\\?;,\\\"!\\$=\\|\\.]", "")
  }

  /**
   * Generate the schema for the DataFrame by using the Variable definitions of the Featureset
   *
   * @param featureset The Featureset
   * @return The schema of the resulting data in the format of StructType
   */
  private def generateSchema(featureset: Featureset): StructType = {
    val fields = featureset.variables.get
      .map(variable =>
        StructField(
          variable.name /*.trim.replaceAll("\\s", "")*/ ,
          if (variable.variable_data_type == VariableDataType.NUMERIC) DoubleType else StringType
        )
      )
    StructType(Seq(StructField("pid", StringType, nullable = false)) ++ fields)
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
        val rowValues = featureset.variables.get.map(variable => { // For each variable
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
   * @param patientURIs       The URIs (/Patient/12323-2312-231) of the patients
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
                                     featureset: Featureset, patientURIs: Set[String]): Future[Map[String, Map[String, Any]]] = {
    val queryFutures = featureset.variables.get
      .map(fetchValuesOfVariable(fhirClient, fhirPathEvaluator, patientURIs, _)) // For each variable, fetch the values for each patient

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
        val criteriaOtherThanPatientResource = eligibility_criteria.filterNot(_.fhir_query.startsWith("/Patient"))
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
              val result = queryResults.reduceLeft((a, b) => a.intersect(b)) // Create a final list bu intersecting the resulting lists coming from criterionOtherThanPatientResource
              logger.debug(s"${result.size} eligible patients are found at page index ${pageIndex}.") // Log the result
              result
            }
        }
      }
    }
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
   * @param patientURIs
   * @param variable
   * @return returns a future in form of the following Map where the Map has only one entry:
   *         Map(Smoking status -> Map(Patient/5dea8608a8273d7cac52005d44a59360 -> 1, Patient/7376b017a75b043a40f0f4ed654852f0 -> 0, Patient/4e6e2d0e0439cbaf75c7f914a5e111d3 -> 0,
   *         Patient/04e5829775e37d63b7a236d9f591e1cb -> 0, Patient/16ed073856be0118031ac67309198420 -> 0, Patient/6602b46629ae17ab1145aaa0fc1f625e -> 0,
   *         Patient/1f02a91bb9343781e02c7f112d7b791d -> 1, Patient/9cd69c88a9526c8a5acabe24504ae497 -> 1, Patient/50605e9eb98ebc79321a9f6b5a8fa0cf -> 1,
   *         Patient/10e5dfbc9116508271574a10beec20a7 -> 0))
   */
  private def fetchValuesOfVariable(fhirClient: FHIRClient, fhirPathEvaluator: FhirPathEvaluator, patientURIs: Set[String],
                                    variable: Variable): Future[Map[String, Map[String, Any]]] = {
    val fhirQuery = QueryHandler.getResourcesOfPatientsQuery(patientURIs, variable.fhir_query, Some(variable.fhir_path))
    fhirQuery.getResources(fhirClient, all = true) map { resources =>
      if (variable.fhir_path.startsWith(FHIRPathExpressionPrefix.AGGREGATION)) {
        // If FHIRPath expression starts with 'FHIRPathExpressionPrefix.AGGREGATION'
        evaluateAggrPath4FeatureSet(fhirPathEvaluator, resources, patientURIs, variable)
      } else if (variable.fhir_path.startsWith(FHIRPathExpressionPrefix.VALUE)) {
        // If FHIRPath expression starts with 'FHIRPathExpressionPrefix.VALUE'
        evaluateValuePath4FeatureSet(fhirPathEvaluator, resources, patientURIs, variable)
      } else {
        // Invalid FHIRPath expression
        logger.error("Invalid FHIRPath expression prefix in featureset variables: {}", variable.fhir_path)
        throw DataPreparationException(s"Invalid FHIRPath expression prefix in featureset variables: ${variable.fhir_path}")
      }
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
  def evaluateAggrPath4FeatureSet(fhirPathEvaluator: FhirPathEvaluator, resources: Seq[JObject], patientURIs: Set[String],
                                  variable: Variable): Map[String, Map[String, Any]] = {

    val isCategoricalType = variable.variable_data_type == VariableDataType.CATEGORICAL
    // If variable data type is Categorical set null otherwise 0
    val initialValue = if (isCategoricalType) null else 0.toDouble
    val initialValuesForAllPatients: Map[String, Any] = patientURIs.map((_ -> initialValue)).toMap

    if (resources.nonEmpty) {
      if (variable.fhir_path.startsWith(FHIRPathExpressionPrefix.AGGREGATION_EXISTENCE)) {
        // Remove FHIR Path expression prefix 'FHIRPathExpressionPrefix.AGGREGATION_EXISTENCE'
        val fhirPathExpression: String = variable.fhir_path.substring(FHIRPathExpressionPrefix.AGGREGATION_EXISTENCE.length)
        // Evaluate FHIR Path on the resource list
        val result = fhirPathEvaluator.evaluateString(fhirPathExpression, JArray(resources.toList))
        val extractedValues = result.map { item =>
          item -> 1.toDouble // Fill with 1 for existing resources
        }.toMap

        Map(variable.name -> (initialValuesForAllPatients ++ extractedValues))
      } else {
        // Remove FHIR Path expression prefix 'FHIRPathExpressionPrefix.AGGREGATION'
        val fhirPathExpression: String = variable.fhir_path.substring(FHIRPathExpressionPrefix.AGGREGATION.length)
        // Evaluate FHIR Path on the resource list
        val result = fhirPathEvaluator.evaluate(fhirPathExpression, JArray(resources.toList))
        val extractedValues = result.map { item =>

          /**
           * complexResult is a List[(a,b)] in which we are sure that there are two tuples as follows:
           * List[(bucket -> JString(Patient/p1), (agg -> JLong(2)))
           */
          val complexResult = item.asInstanceOf[FhirPathComplex] // retrieve as a complex result
            .json.obj // Access to the List[(String, JValue)]

          import ppddm.core.util.JsonFormatter._

          val patientID: String = complexResult.filter(_._1 == "bucket").head._2.extract[String]
          val aggrResult: Double = complexResult.filter(_._1 == "agg").head._2.extract[Double]
          patientID -> aggrResult // Patient ID -> count
        }.toMap

        Map(variable.name -> (initialValuesForAllPatients ++ extractedValues))
      }
    } else {
      Map(variable.name -> initialValuesForAllPatients)
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
   */
  def evaluateValuePath4FeatureSet(fhirPathEvaluator: FhirPathEvaluator, resources: Seq[JObject], patientURIs: Set[String],
                                   variable: Variable): Map[String, Map[String, Any]] = {

    val isCategoricalType = variable.variable_data_type == VariableDataType.CATEGORICAL
    // If variable data type is Categorical set null otherwise 0
    val initialValue = if (isCategoricalType) null else 0.toDouble
    val initialValuesForAllPatients: Map[String, Any] = patientURIs.map((_ -> initialValue)).toMap

    // Remove FHIR Path expression prefix 'FHIRPathExpressionPrefix.VALUE'
    val fhirPathExpression: String = variable.fhir_path.substring(FHIRPathExpressionPrefix.VALUE.length)

    val isPatient = variable.fhir_query.startsWith("/Patient")
    val lookingForExistence: Boolean = fhirPathExpression.startsWith("exists")

    val extractedValues = resources
      .map { item => // For each resulting resource
        /**
         * Set the value null if the variable data type is categorical or the fhir_path is not looking for existence.
         * e.g. Observation.valueQuantity.value field is numeric and fhir_path is not looking for existence,
         * and if its fhir_path evaluation result is None, the value should be null.
         * Setting the value 1 means that the variable is looking for the existence, and its default value is 1 if it exists among resources.
         */
        var value = if (isCategoricalType || !lookingForExistence) null else 1.toDouble
        if (!lookingForExistence) {
          val resultOption = Try(fhirPathEvaluator.evaluate(fhirPathExpression, item).headOption).getOrElse(None)
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
        // If it is Patient resource get the id from Patient.id. Otherwise, get it from [Resource].subject.reference
        if (isPatient) {
          "Patient/" + (item \ "id").asInstanceOf[JString].values -> value
        } else {
          (item \ "subject" \ "reference").asInstanceOf[JString].values -> value
        }
      }
      .toMap // Convert to Map[String, Any]
    Map(variable.name -> (initialValuesForAllPatients ++ extractedValues))
  }

  /**
   * Retrieves the DataPreparationResult which inlcudes the DataSourceStatistics for the given dataset_id
   *
   * @param dataset_id
   * @return
   */
  def getDataSourceStatistics(dataset_id: String): Option[DataPreparationResult] = {
    logger.debug("DataSourceStatistics is requested for the Dataset:{}", dataset_id)
    Try(
      DataStoreManager.getDataFrame(DataStoreManager.getStatisticsPath(dataset_id)) map { df =>
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
    DataStoreManager.deleteDirectory(DataStoreManager.getDatasetPath(dataset_id))
    // Delete the statistics related with the given dataset_id
    val statisticsDeleted = DataStoreManager.deleteDirectory(DataStoreManager.getStatisticsPath(dataset_id))
    if (statisticsDeleted) {
      logger.info(s"Dataset and statistics (with id: $dataset_id) have been deleted successfully")
      Some(Done)
    } else {
      logger.info(s"Dataset and statistics (with id: $dataset_id) do not exist!")
      Option.empty[Done]
    }
  }

}
