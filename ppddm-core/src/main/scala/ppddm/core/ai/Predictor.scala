package ppddm.core.ai

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.functions.{col, udf, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This object handles the calculation of prediction of BoostedModel consisting of a number of WeakModels
 */
object Predictor {

  private val logger: Logger = Logger(this.getClass)

  private def generateWeightedPredictionAndProbabilities(testPredictionTuples: Seq[(Double, DataFrame)])(implicit sparkSession: SparkSession): DataFrame = {
    logger.debug("Generating weighted prediction and probabilities...")

    import sparkSession.implicits._

    val vectorToArray = udf( (xs: org.apache.spark.ml.linalg.Vector) => xs.toArray ) // To convert Vector to Array in "probability" column

    if (testPredictionTuples.length == 1) { // We have only one DataFrame, return it directly as we don't need to do any calculation
      testPredictionTuples.head._2
        .withColumn("weightedPrediction", col("prediction"))
        .withColumn("probabilityArray", vectorToArray($"probability"))
        .select($"pid", $"label", $"weightedPrediction",
          $"probabilityArray".getItem(0).as("negativeProbability"),
          $"probabilityArray".getItem(1).as("positiveProbability"))
    } else { // Predict with weighted average of predictions
      // Introduce three new columns: "weightedPrediction", "negativeProbability", "positiveProbability"
      // "weightedPrediction" with 0.0 predictions as negative and 1.0 predictions as positive by also multiplying them with the weight of corresponding weak model
      // "negativeProbability" and "positiveProbability" with probabilities multiplied by the weight of corresponding weak model
      val weightedPredictionAndProbabilityDFs = testPredictionTuples.map { t =>
        t._2.withColumn("weightedPrediction", when(col("prediction") === 0.0, -1 * t._1).otherwise(t._1))
            .withColumn("probabilityArray", vectorToArray($"probability"))
            .select($"pid", $"label", $"weightedPrediction",
              $"probabilityArray".getItem(0).multiply(t._1).as("negativeProbability"),
              $"probabilityArray".getItem(1).multiply(t._1).as("positiveProbability"))
      }

      // Then take the sum of each weightedPrediction, negativeProbability and positiveProbability row-wise
      val predictedDF = weightedPredictionAndProbabilityDFs.reduceLeft { (a, b) =>
        val table1 = a.as("table1")
        val table2 = b.as("table2")
        table1.join(table2, $"table1.pid" === $"table2.pid") // Join two tables with the pid as it is uniqueID
          .select($"table1.pid", $"table1.label", $"table1.weightedPrediction" as "wp1", $"table2.weightedPrediction" as "wp2",
            $"table1.negativeProbability" as "n1", $"table2.negativeProbability" as "n2",
            $"table1.positiveProbability" as "p1", $"table2.positiveProbability" as "p2") // Introduce temporary wp1, wp2, n1, n2, p1 and p2 columns
          .withColumn("weightedPrediction", col("wp1") + col("wp2")) // Sum these values and write to "weightedPrediction" column
          .withColumn("negativeProbability", col("n1") + col("n2")) // Sum negative probabilities and write to "negativeProbability" column
          .withColumn("positiveProbability", col("p1") + col("p2")) // Sum positive probabilities and write to "positiveProbability" column
          .drop("wp1", "wp2", "n1", "n2", "p1", "p2") // Drop the temporary columns
      }

      logger.debug("Finish generating weighted prediction and probabilities.")

      predictedDF
    }
  }

  /**
   * Predict values by taking weighted average of predictions of each weak model
   * @param testPredictionTuples
   * @return
   */
  def predictWithWeightedAverageOfPredictions(testPredictionTuples: Seq[(Double, DataFrame)])(implicit sparkSession: SparkSession): DataFrame = {
    logger.debug("## Start predicting with weighted average of predictions ##")

    // The idea is to make 0.0 prediction negative and 1.0 prediction positive, and multiply each prediction
    // with the weight of corresponding weak model, and then take sum of all values. If the result is negative
    // or equal to zero, then predict 0.0, otherwise predict 1.0
    // TODO consider equal to zero case. Predict 0.0 or 1.0?

    val predictedDF = generateWeightedPredictionAndProbabilities(testPredictionTuples)

    logger.debug("## Finish predicting with weighted average of predictions ##")

    // If the result is negative or equal to zero, then predict 0.0, otherwise predict 1.0
    // TODO consider equal to zero case. Predict 0.0 or 1.0?
    predictedDF.withColumn("prediction", when(col("weightedPrediction") <= 0.0, 0.0).otherwise(1.0))
  }

  /**
   * Predict values by comparing weighted sum of probabilities of predictions of each weak model
   * @param testPredictionTuples
   * @return
   */
  def predictWithWeightedProbability(testPredictionTuples: Seq[(Double, DataFrame)])(implicit sparkSession: SparkSession): DataFrame = {
    logger.debug("## Start predicting with weighted probabilities ##")

    // The idea is to take negative and positive probabilities, multiply each probability with
    // the weight of corresponding weak model, and then take sum of all negative and positive probabilities.
    // At the end of this, if the negative prediction is bigger than or equal to positive prediction, then predict 0.0, otherwise predict 1.0
    // TODO consider the case of equality of negative and positive probabilities. Predict 0.0 or 1.0?

    val predictedDF = generateWeightedPredictionAndProbabilities(testPredictionTuples)

    logger.debug("## Finish predicting with weighted probabilities ##")

    // If the negative prediction is bigger than or equal to positive prediction, then predict 0.0, otherwise predict 1.0
    // TODO consider the case of equality of negative and positive probabilities. Predict 0.0 or 1.0?
    predictedDF.withColumn("prediction", when(col("negativeProbability") >= col("positiveProbability"), 0.0).otherwise(1.0))

  }

  /**
   * Predict values by finding the most frequent prediction made by all weak models
   * @param testPredictionTuples
   * @return
   */
  def predictWithMajorityVoting(testPredictionTuples: Seq[(Double, DataFrame)])(implicit sparkSession: SparkSession): DataFrame = {
    logger.debug("## Start predicting with majority voting ##")

    import sparkSession.implicits._

    if (testPredictionTuples.length == 1) { // We have only one DataFrame, return it directly as we don't need to do any calculation
      testPredictionTuples.head._2
    } else { // Predict with majority voting
      // The idea is to look the predictions of all the weak models and find the one that are predicted most.
      // If the number of 0.0 predictions is equal to the number of 1.0 predictions, predict 0.0
      // TODO In equality case, predict 0.0 or 1.0?

      // First introduce a new column "signedPrediction" with 0.0 predictions as negative and 1.0 predictions as positive
      val testPredictionDFsWithSign = testPredictionTuples.map { t =>
        t._2.withColumn("signedPrediction", when(col("prediction") === 0.0, -1.0).otherwise(1.0))
      }

      // Then take the sum of each signed prediction row-wise
      val predictedDF = testPredictionDFsWithSign.reduceLeft { (a, b) =>
        val table1 = a.as("table1")
        val table2 = b.as("table2")
        table1.join(table2, $"table1.pid" === $"table2.pid") // Join two tables with the pid as it is uniqueID
          .select($"table1.pid", $"table1.label", $"table1.signedPrediction" as "p1", $"table2.signedPrediction" as "p2") // Introduce temporary p1 and p2 columns
          .withColumn("signedPrediction", col("p1") + col("p2")) // Sum these values and write to "signedPrediction" column
          .drop("p1", "p2") // Drop the temporary columns
      }

      logger.debug("## Finish predicting with majority voting ##")

      // If the value is negative or equal to 0.0, then predict 0.0. Otherwise, predict 1.0
      // TODO In equality case, predict 0.0 or 1.0?
      predictedDF.withColumn("prediction", when(col("signedPrediction") <= 0.0, 0.0).otherwise(1.0))
    }
  }

}
