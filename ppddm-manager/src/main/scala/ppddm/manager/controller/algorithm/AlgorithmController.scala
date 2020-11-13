package ppddm.manager.controller.algorithm

import ppddm.core.rest.model.{Algorithm, AlgorithmName, AlgorithmParameterName, DataType, Parameter}

/**
 * Controller object for the Algorithms supported by the PPDDM architecture
 */
object AlgorithmController {

  /**
   * Retrieves all available algorithms to be used for the Data Mining operations. The Algorithms have also their parameter
   * definitions indicating some default values.
   *
   * @return a Sequence of Algorithms
   */
  def getAvailableAlgorithms(): Seq[Algorithm] = {

    val num_folds = Parameter(AlgorithmParameterName.NUMBER_OF_FOLDS, DataType.INTEGER, "3")
    val max_parallelism = Parameter(AlgorithmParameterName.MAX_PARALLELISM, DataType.INTEGER, "2")
    val metric = Parameter(AlgorithmParameterName.METRIC, DataType.STRING, "areaUnderROC")
    val threshold = Parameter(AlgorithmParameterName.THRESHOLD, DataType.DOUBLE, "0.5")
    val max_iter = Parameter(AlgorithmParameterName.MAX_ITER, DataType.INTEGER, "10")
    val reg_param = Parameter(AlgorithmParameterName.REG_PARAM, DataType.DOUBLE, "0.0")
    val elasticnet_param = Parameter(AlgorithmParameterName.ELASTIC_NET_PARAM, DataType.DOUBLE, "0.0")
    val max_depth = Parameter(AlgorithmParameterName.MAX_DEPTH, DataType.INTEGER, "5")
    val min_info_gain = Parameter(AlgorithmParameterName.MIN_INFO_GAIN, DataType.DOUBLE, "0.0")
    val impurity = Parameter(AlgorithmParameterName.IMPURITY, DataType.STRING, "gini")
    val num_trees = Parameter(AlgorithmParameterName.NUM_TREES, DataType.INTEGER, "5")
    val feature_subset_strategy = Parameter(AlgorithmParameterName.FEATURE_SUBSET_STRATEGY, DataType.STRING, "auto")

    val parametersForAllAlgorithms = Seq(num_folds, max_parallelism, metric)

    Seq(
      Algorithm(AlgorithmName.ARL_PREFIX_SPAN, parametersForAllAlgorithms),
      Algorithm(AlgorithmName.ARL_FPGROWTH, parametersForAllAlgorithms),
      Algorithm(AlgorithmName.CLASSIFICATION_SVM, parametersForAllAlgorithms ++ Seq(max_iter)),
      Algorithm(AlgorithmName.CLASSIFICATION_LOGISTIC_REGRESSION, parametersForAllAlgorithms ++ Seq(threshold, max_iter, reg_param, elasticnet_param)),
      Algorithm(AlgorithmName.CLASSIFICATION_DECISION_TREE, parametersForAllAlgorithms ++ Seq(max_depth, min_info_gain, impurity)),
      Algorithm(AlgorithmName.CLASSIFICATION_RANDOM_FOREST, parametersForAllAlgorithms ++ Seq(max_depth, min_info_gain, impurity, num_trees, feature_subset_strategy)),
      Algorithm(AlgorithmName.CLASSIFICATION_GBT, parametersForAllAlgorithms ++ Seq(max_iter, max_depth, min_info_gain, feature_subset_strategy)),
      Algorithm(AlgorithmName.CLASSIFICATION_NAIVE_BAYES, parametersForAllAlgorithms),
      Algorithm(AlgorithmName.REGRESSION_LINEAR, parametersForAllAlgorithms ++ Seq(threshold, max_iter, reg_param, elasticnet_param)),
      Algorithm(AlgorithmName.REGRESSION_DECISION_TREE, parametersForAllAlgorithms ++ Seq(threshold, max_iter, reg_param, elasticnet_param)),
      Algorithm(AlgorithmName.REGRESSION_RANDOM_FOREST, parametersForAllAlgorithms ++ Seq(threshold, max_iter, reg_param, elasticnet_param)),
      Algorithm(AlgorithmName.REGRESSION_GBT, parametersForAllAlgorithms ++ Seq(threshold, max_iter, reg_param, elasticnet_param))
    )
  }

}
