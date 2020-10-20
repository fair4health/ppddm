package ppddm.agent.controller.dm

import org.apache.spark.sql.{DataFrame, SparkSession}
import ppddm.agent.Agent
import org.apache.spark.sql.functions.{when, _}

object BoostedModelManager {

  private val sparkSession: SparkSession = Agent.dataMiningEngine.sparkSession
  import sparkSession.implicits._

  def predictWithWeightedAverageOfPredictions(testPredictionTuples: Seq[(Double, DataFrame)]): DataFrame = {
    if (testPredictionTuples.length == 1) { // We have only one DataFrame, return it directly as we don't need to do any calculation
      testPredictionTuples.head._2
    } else { // Predict with weighted average of predictions
      // The idea is to make 0.0 prediction negative and 1.0 prediction positive, and multiply each prediction
      // with the weight of corresponding weak model, and then take sum of all values. If the result is negative
      // or equal to zero, then predict 0.0, otherwise predict 1.0
      // TODO consider equal to zero case. Predict 0.0 or 1.0?


      // First introduce a new column "weightedPrediction" with 0.0 predictions as negative and 1.0 predictions
      // as positive by also multiplying them with the weight of corresponding weak model
      val weightPredictionDFs = testPredictionTuples.map { t =>
        t._2.withColumn("weightedPrediction", when(col("prediction") === 0.0, -1 * t._1).otherwise(t._1))
      }

      // Then take the sum of each weighted prediction row-wise
      val predictedDF = weightPredictionDFs.reduceLeft { (a, b) =>
        val table1 = a.as("table1")
        val table2 = b.as("table2")
        table1.join(table2, $"table1.pid" === $"table2.pid") // Join two tables with the pid as it is uniqueID
          .select($"table1.pid", $"table1.label", $"table1.weightedPrediction" as "p1", $"table2.weightedPrediction" as "p2") // Introduce temporary p1 and p2 columns
          .withColumn("weightedPrediction", col("p1") + col("p2")) // Sum these values and write to "weightedPrediction" column
            .drop("p1", "p2") // Drop the temporary columns
      }

      // If the result is negative or equal to zero, then predict 0.0, otherwise predict 1.0
      // TODO consider equal to zero case. Predict 0.0 or 1.0?
      predictedDF.withColumn("prediction", when(col("weightedPrediction") <= 0.0, 0.0).otherwise(1.0))

    }
  }

  def predictWithWeightedProbability(testPredictionDFs: Seq[DataFrame]): DataFrame = {
    testPredictionDFs.map { testPredictionDF =>

    }
    null
  }

  def predictWithMajorityVoting(testPredictionDFs: Seq[DataFrame]): DataFrame = {
    testPredictionDFs.map { testPredictionDF =>

    }
    null
  }

}
