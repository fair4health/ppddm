package ppddm.core.ai

import com.typesafe.scalalogging.Logger
import ppddm.core.rest.model.{AlgorithmStatisticsName, DataMiningModel}

/**
 * This object handles the calculation of training statistics and weights of weak models for a boosted model
 */
object Aggregator {

  private val logger: Logger = Logger(this.getClass)

  /**
   * 
   * @param dataMiningModel
   * @return
   */
  def aggregate(dataMiningModel: DataMiningModel): DataMiningModel = {
    logger.debug("## Start aggregating ##")

    logger.debug("Aggregating for each boosted model...")
    val updatedBoostedModels = dataMiningModel.boosted_models.get map { boostedModel =>
      var weightSum = 0.0 // For calculating normalized weight at the end

      logger.debug("In a boosted model, calculating calculated_training_statistics for each weak model...")
      val weakModelsWithCalculatedStatistics = boostedModel.weak_models map { weakModel =>
        // We call .get on these Option's (validation_statistics and training_statistics because they MUST exist if this function is called
        val calculatedStatistics = StatisticsCalculator.combineClassificationStatistics(weakModel.validation_statistics.get :+ weakModel.training_statistics.get) // Combine training_statistics in WeakModel
        val weight = StatisticsCalculator.getStatisticsValue(calculatedStatistics, AlgorithmStatisticsName.F_MEASURE).toDouble // TODO read this from config file and find a better way to calculate this
        weightSum += weight // We need sum of all weights in a BoostedModel so that we can normalize the weights
        weakModel.withCalculatedStatistics(calculatedStatistics).withWeight(weight)
      }
      logger.debug("Finished calculating calculated_training_statistics.")
      val updatedBoostedModel = boostedModel.replaceWeakModels(weakModelsWithCalculatedStatistics)

      logger.debug("In a boosted model, calculating normalized weight of each weak model...")
      val weakModelsWithNormalizedWeight = updatedBoostedModel.weak_models map { weakModel =>
        val normalizedWeight = weakModel.weight.get / weightSum // Normalize the weights
        logger.debug(s"Normalized weight of a weak model: ${normalizedWeight}")
        weakModel.withWeight(normalizedWeight)
      }
      logger.debug("Finished calculating weights.")
      updatedBoostedModel.replaceWeakModels(weakModelsWithNormalizedWeight)
    }

    logger.debug("## Finish aggregating ##")

    dataMiningModel.withBoostedModels(updatedBoostedModels)
  }

}
