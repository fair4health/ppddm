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
      val weakModelsWithCalculatedTrainingStatistics = boostedModel.weak_models map { weakModel =>
        val calculatedStatistics = StatisticsCalculator.combineStatistics(weakModel.training_statistics) // Combine training_statistics in WeakModel
        val weight = StatisticsCalculator.getStatisticsValue(calculatedStatistics, AlgorithmStatisticsName.F_MEASURE).toDouble // TODO read this from config file and find a better way to calculate this
        weightSum += weight // We need sum of all weights in a BoostedModel so that we can normalize the weights
        weakModel.withCalculatedTrainingStatistics(calculatedStatistics).withWeight(weight)
      }
      logger.debug("Finished calculating calculated_training_statistics.")
      val updatedBoostedModel = boostedModel.replaceWeakModels(weakModelsWithCalculatedTrainingStatistics)

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
