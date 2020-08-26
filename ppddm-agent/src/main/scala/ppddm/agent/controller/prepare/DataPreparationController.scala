package ppddm.agent.controller.prepare

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.Logger
import io.onfhir.path._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.json4s.JsonAST.JObject
import org.json4s.{DefaultFormats, JArray, JString}
import ppddm.agent.Agent
import ppddm.agent.config.AgentConfig
import ppddm.agent.spark.NodeExecutionContext._
import ppddm.core.fhir.{FHIRClient, FHIRQuery}
import ppddm.core.rest.model._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, TimeoutException}


/**
 * Controller object for data preparation.
 *
 * PPDDM Manager requests data preparation together with the unique identifier of the dataset on the platform.
 * After the submission, the manager checks whether the data is prepared or not by asking for the results.
 */
object DataPreparationController {

  private val logger: Logger = Logger(this.getClass)

  private val batchSize: Int = AgentConfig.agentBatchSize
  private val sparkSession: SparkSession = Agent.dataMiningEngine.sparkSession

  implicit val formats: DefaultFormats = DefaultFormats

  /**
   * Start the data preparation (data extraction process) with the given DataPreparationRequest.
   * This function successfully returns if the preparation request is started. Data preparation will continue in the background
   * and the clients will ask about the execution status through a separate API call.
   *
   * @param dataPreparationRequest The request object for the data preparation.
   * @return
   */
  def startPreparation(dataPreparationRequest: DataPreparationRequest): Future[Set[String]] = {
    // TODO: We may need some validation on the DataPreparationRequest object
    prepareData(dataPreparationRequest)
  }

  private def prepareData(dataPreparationRequest: DataPreparationRequest): Future[Set[String]] = {
    logger.debug("Data preparation request received.")

    val fhirClientMaster = FHIRClient(AgentConfig.fhirHost, AgentConfig.fhirPort, AgentConfig.fhirPath, AgentConfig.fhirProtocol)

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
            val fhirClientPartition = FHIRClient(AgentConfig.fhirHost, AgentConfig.fhirPort, AgentConfig.fhirPath, AgentConfig.fhirProtocol)
            val fhirPathEvaluator = FhirPathEvaluator()

            // Fetch the Patient resources from the FHIR Repository and collect their IDs
            val theFuture = findEligiblePatients(fhirClientPartition, fhirPathEvaluator, dataPreparationRequest.eligibility_criteria, patientQuery, pageIndex) flatMap { eligiblePatientURIs =>
              if (eligiblePatientURIs.isEmpty) {
                // No patients are eligible
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
                logger.error("The data preparation cannot be completed on a worker node with pageIndex:{} within 10 minutes.", pageIndex, e)
                Seq.empty[Row] // TODO: Check whether we can do better than returning an empty sequence. What happens if a worker node throws an exception?
            }
          }
        })

        // RDDs are lazy evaluated. In order to materialize the above statement, call an action on rdd such as foreach, collect, count etc.
        val dataRowSet = rdd.collect() // Collect the Seq[Row]s from the worker nodes
          .toSeq // Covert the Array[Seq[Row]]s to Seq[Seq[Row]]s
          .flatten // Create a single Seq[Row] by merging all Seq[Row]s
        // TODO If you are going to use the same RDD more than once, make sure to call rdd.cache() first. Otherwise, it will be executed in each action
        // TODO When you are done, call rdd.unpersist() to remove it from cache.

        logger.debug("Data is collected from the worker nodes. And now the DataFrame will be constructed.")

        val structureSchema = generateSchema(dataPreparationRequest.featureset)

        val dataFrame = sparkSession.createDataFrame(
          sparkSession.sparkContext.parallelize(dataRowSet), // After collecting the data from the worker nodes, parallelize it again
          structureSchema)

        logger.debug(dataFrame.schema.treeString)
        dataFrame.show(false)

        Set.empty[String]

      } else {
        logger.info("There are no patients for the given eligibility criteria: {}", dataPreparationRequest.eligibility_criteria)
        Set.empty[String]
      }
    }

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
          variable.name/*.replaceAll("\\s", "")*/,
          if (variable.variable_data_type == VariableDataType.NUMERIC) IntegerType else StringType
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
          resourceForVariable(patientURI) // Find the corresponding patient and write the value to the corresponding cell.
        })
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
   *                                   Patient/04e5829775e37d63b7a236d9f591e1cb -> 0, Patient/16ed073856be0118031ac67309198420 -> 0, Patient/6602b46629ae17ab1145aaa0fc1f625e -> 0,
   *                                   Patient/1f02a91bb9343781e02c7f112d7b791d -> 1, Patient/9cd69c88a9526c8a5acabe24504ae497 -> 1, Patient/50605e9eb98ebc79321a9f6b5a8fa0cf -> 1,
   *                                   Patient/10e5dfbc9116508271574a10beec20a7 -> 0),
   *             Number of prescribed drugs -> Map(Patient/5dea8608a8273d7cac52005d44a59360 -> 0, Patient/7376b017a75b043a40f0f4ed654852f0 -> 0, Patient/4e6e2d0e0439cbaf75c7f914a5e111d3 -> 0,
   *                                               Patient/04e5829775e37d63b7a236d9f591e1cb -> 0, Patient/16ed073856be0118031ac67309198420 -> 0, Patient/6602b46629ae17ab1145aaa0fc1f625e -> 0,
   *                                               Patient/1f02a91bb9343781e02c7f112d7b791d -> 0, Patient/9cd69c88a9526c8a5acabe24504ae497 -> 0, Patient/50605e9eb98ebc79321a9f6b5a8fa0cf -> 0,
   *                                               Patient/10e5dfbc9116508271574a10beec20a7 -> 0))
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
    patientQuery.getResources(fhirClient, batchSize, pageIndex) flatMap { results =>

      // If fhir_path is non-empty, filter resulting patients which satisfy the FHIR path expression
      val patients = results.filter(patientQuery.getFHIRPath().isEmpty || fhirPathEvaluator.satisfies(patientQuery.getFHIRPath().get, _))

      val patientURIs: Set[String] = patients
        .map(p => (p \ "id").asInstanceOf[JString].values) // extract the IDs from the JObject of Patient
        .map(pid => s"Patient/$pid") // Prepend the Patient keyword to each pid
        .toSet[String] // Convert to a set

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
              val result = queryResults.reduceLeft((a, b) => a.intersect(b)) // Create a final list bu intersecting the resulting lists
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
    fhirQuery.getResources(fhirClient) map { resources =>
      resources
        .filter(eligibilityCriteria.fhir_path.isEmpty // If fhir_path is non-empty, check if the given resource satisfies the FHIR path expression
          || fhirPathEvaluator.satisfies(eligibilityCriteria.fhir_path.get, _))
        .map(r => (r \ "subject" \ "reference").asInstanceOf[JString].values) // Collect the patientIDs
        .toSet // Convert to set
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
    fhirQuery.getResources(fhirClient) map { resources =>

      if (variable.fhir_path.startsWith(FHIRPathExpressionPrefix.AGGREGATION)) {
        // If FHIRPath expression starts with 'FHIRPathExpressionPrefix.AGGREGATION'
        evaluateAggrPath(fhirPathEvaluator, resources, patientURIs, variable)

      } else if (variable.fhir_path.startsWith(FHIRPathExpressionPrefix.VALUE)) {
        // If FHIRPath expression starts with 'FHIRPathExpressionPrefix.VALUE'
        evaluateValuePath(fhirPathEvaluator, resources, patientURIs, variable)

      } else { // Invalid FHIRPath expression
        Map(variable.name -> patientURIs.map((_ -> 0)).toMap)
      }
    }
  }

  def evaluateAggrPath(fhirPathEvaluator: FhirPathEvaluator, resources: Seq[JObject], patientURIs: Set[String],
                       variable: Variable): Map[String, Map[String, Any]] = {
    val map1: Map[String, Any] = patientURIs.map((_ -> 0)).toMap
    // Remove FHIR Path expression prefix 'FHIRPathExpressionPrefix.AGGREGATION'
    val fhirPathExpression: String = variable.fhir_path.substring(FHIRPathExpressionPrefix.AGGREGATION.length)
    // Evaluate FHIR Path on the resource list
    val result = fhirPathEvaluator.evaluate(fhirPathExpression, JArray(resources.toList))

    val map2 = map1 ++ result.map { item =>
      val a = item.asInstanceOf[FhirPathComplex].json.obj
      (a.head._2.extract[String] -> a(1)._2.extract[Int]) // Patient ID -> count
    }.toMap
    Map(variable.name -> map2)
  }

  def evaluateValuePath(fhirPathEvaluator: FhirPathEvaluator, resources: Seq[JObject], patientURIs: Set[String],
                        variable: Variable): Map[String, Map[String, Any]] = {
    val map1: Map[String, Any] = patientURIs.map((_ -> 0)).toMap
    // Remove FHIR Path expression prefix 'FHIRPathExpressionPrefix.VALUE'
    val fhirPathExpression: String = variable.fhir_path.substring(FHIRPathExpressionPrefix.VALUE.length)

    val lookingForExistence: Boolean = fhirPathExpression.startsWith("exists")

    val map2 = map1 ++ resources.map { r =>
      var value: Any = 1
      if (!lookingForExistence) { // TODO: generalize it for all. Currently, only for existence check
        val result = fhirPathEvaluator.evaluate(fhirPathExpression, r).head
        value = result match {
          case FhirPathString(s) => s
          case FhirPathNumber(v) => v
          case FhirPathDateTime(dt) => dt
          case _ => "-"
        }
      }

      // If it is Patient resource get the id from Patient.id. Otherwise, get it from [Resource].subject.reference
      if (variable.fhir_query.startsWith("/Patient")) {
        ("Patient/" + (r \ "id").asInstanceOf[JString].values -> value)
      } else {
        ((r \ "subject" \ "reference").asInstanceOf[JString].values -> value)
      }

    }.toMap
    Map(variable.name -> map2)
  }

  def getDataSourceStatistics(dataset_id: String): Future[Option[DataPreparationResult]] = {
    Future {
      None
    }
  }

}
