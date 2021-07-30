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

    val num_folds = Parameter(AlgorithmParameterName.NUMBER_OF_FOLDS, DataType.INTEGER, 3,
      display = "Number of folds",
      description = "Value of k in k-fold Cross validation. Use 3+ in practice. K-fold cross validation performs model selection by " +
        "splitting the dataset into a set of non-overlapping randomly partitioned folds which are used as separate " +
        "training and test datasets e.g., with k=3 folds, K-fold cross validation will generate 3 (training, test) " +
        "dataset pairs, each of which uses 2/3 of the data for training and 1/3 for testing.",
      possible_values_description = "It can be any integer value greater than or equal to 2.",
      comma_separated_multiple_values = false,
      possible_values = None)

    val max_parallelism = Parameter(AlgorithmParameterName.MAX_PARALLELISM, DataType.INTEGER, "2",
      display = "Maximum level of parallelism",
      description = "The maximum level of parallelism to evaluate models in parallel. 1 means serial evaluation. This is an advanced setting. " +
        "In case you have limited knowledge, just use 2.",
      possible_values_description = "It can be any integer value greater than or equal to 1.",
      comma_separated_multiple_values = false,
      possible_values = None)

    val metric = Parameter(AlgorithmParameterName.METRIC, DataType.STRING, "areaUnderROC",
      display = "Metric to use on Cross validation",
      description = "Metric to use on Cross validation while evaluating models",
      possible_values_description = "Use either \"areaUnderROC\" or \"areaUnderPR\"",
      comma_separated_multiple_values = false,
      possible_values = Some(Seq("areaUnderROC", "areaUnderPR")))

    val handle_invalid = Parameter(AlgorithmParameterName.HANDLE_INVALID, DataType.STRING, "keep",
      display = "How to handle invalid data",
      description = "Parameter for how to handle invalid data (unseen labels or NULL values). Options are \"keep\", " +
        "\"error\" or \"skip\". \"keep\" puts unseen labels in a special additional bucket, at index numLabels. \"error\" " +
        "throws an error when there is a null value. \"skip\" removes the row directly.",
      possible_values_description = "Use either \"keep\" or \"error\" or \"skip\"",
      comma_separated_multiple_values = false,
      possible_values = Some(Seq("keep", "error", "skip")))

    val imputation_strategy = Parameter(AlgorithmParameterName.IMPUTATION_STRATEGY, DataType.STRING, "median",
      display = "Imputation strategy",
      description = "The imputation strategy used when handling invalid data with \"keep\" option. " +
        "Possible values are \"mean\" and \"median\". If \"mean\", then replace missing values using the mean value of " +
        "the feature. If \"median\", then replace missing values using the approximate median value of the feature.",
      possible_values_description = "User either \"mean\" or \"median\"",
      comma_separated_multiple_values = false,
      possible_values = Some(Seq("mean", "median")))

    val threshold = Parameter(AlgorithmParameterName.THRESHOLD, DataType.DOUBLE, 0.5f,
      display = "Threshold",
      description = "Param for threshold in binary classification prediction, in range [0, 1].",
      possible_values_description = "It can be any double value in range [0.0,1.0]",
      comma_separated_multiple_values = true,
      possible_values = None)

    val max_iter = Parameter(AlgorithmParameterName.MAX_ITER, DataType.INTEGER, 100,
      display = "Maximum number of iterations",
      description = "Param for maximum number of iterations taken for the solvers to converge (>= 0).",
      possible_values_description = "It can be any integer value greater than or equal to 0.",
      comma_separated_multiple_values = true,
      possible_values = None)


    val reg_param = Parameter(AlgorithmParameterName.ELASTIC_NET_PARAM, DataType.DOUBLE, 0.0f,
      display = "ElasticNet mixing parameter",
      description = "The ElasticNet mixing parameter. For alpha = 0, the penalty is an L2 penalty (Ridge regression model). " +
        "For alpha = 1, it is an L1 penalty (Lasso model). For 0 < alpha < 1, the penalty is a combination of L1 and L2. " +
        "Default is 0.0 which is an L2 penalty.",
      possible_values_description = "It can be any double value in range [0.0,1.0]",
      comma_separated_multiple_values = true,
      possible_values = None)

    val elasticnet_param = Parameter(AlgorithmParameterName.REG_PARAM, DataType.DOUBLE, 0.0f,
      display = "Regularization parameter",
      description = "The regularization parameter. Default is 0.0.",
      possible_values_description = "It can be any double value in range [0.0,1.0]",
      comma_separated_multiple_values = true,
      possible_values = None)

    val max_depth = Parameter(AlgorithmParameterName.MAX_DEPTH, DataType.INTEGER, 5,
      display = "Maximum depth of a tree",
      description = "Maximum depth of the tree (nonnegative). E.g., depth 0 means 1 leaf node; depth 1 means 1 internal " +
        "node + 2 leaf nodes. (default = 5). Deeper trees are more expressive (potentially allowing higher accuracy), " +
        "but they are also more costly to train and are more likely to overfit.",
      possible_values_description = "It can be any integer value greater than or equal to 0.",
      comma_separated_multiple_values = true,
      possible_values = None)

    val min_info_gain = Parameter(AlgorithmParameterName.MIN_INFO_GAIN, DataType.DOUBLE, 0.0f,
      display = "Minimum information gain",
      description = "Minimum information gain for a split to be considered at a tree node. Should be at least 0.0. " +
        "(default = 0.0). For a node to be split further, the split must improve at least this much (in terms of information gain).",
      possible_values_description = "It can be any double value greater than or equal to 0.0",
      comma_separated_multiple_values = true,
      possible_values = None)

    val impurity = Parameter(AlgorithmParameterName.IMPURITY, DataType.STRING, "gini",
      display = "Impurity",
      description = "Criterion used for information gain calculation. The node impurity is a measure of the homogeneity " +
        "of the labels at the node. Values can be \"gini\" and/or \"entropy\".",
      possible_values_description = "User either \"gini\" and/or \"entropy\"",
      comma_separated_multiple_values = true,
      possible_values = Some(Seq("gini", "entropy"))
    )

    val num_trees = Parameter(AlgorithmParameterName.NUM_TREES, DataType.INTEGER, 50,
      display = "Number of trees",
      description = "Number of trees in the forest. Increasing the number of trees will decrease the variance in predictions, improving the model’s test-time accuracy.",
      possible_values_description = "It can be any integer value greater than or equal to 1.",
      comma_separated_multiple_values = true,
      possible_values = None)

    val feature_subset_strategy = Parameter(AlgorithmParameterName.FEATURE_SUBSET_STRATEGY, DataType.STRING, "auto",
      display = "Feature subset strategy",
      description = "\"Number of features to use as candidates for splitting at each tree node. Default is \"\"auto\"\". " +
        "Supported options are :\n- \"\"auto\"\": Choose automatically for task: If numTrees == 1, set to \"\"all.\"\" " +
        "If numTrees greater than 1 (forest), set to \"\"sqrt\"\" for classification and to \"\"onethird\"\" for " +
        "regression.\n- \"\"all\"\": use all features\n- \"\"onethird\"\": use 1/3 of the features\n- \"\"sqrt\"\": use " +
        "sqrt(number of features)\n- \"\"log2\"\": use log2(number of features)\n- \"\"n\"\": when n is in the range (0, 1.0], " +
        "use n * number of features. When n is in the range (1, number of features), use n features. (default = \"\"auto\"\")\"",
      possible_values_description = "You can use these values: \"auto\", \"all\", \"onethird\", \"sqrt\", \"log2\", \"n\"",
      comma_separated_multiple_values = true,
      possible_values = Some(Seq("auto", "all", "onethird", "sqrt", "log2", "n")))
    
    val min_support = Parameter(AlgorithmParameterName.MIN_SUPPORT, DataType.DOUBLE, 0.3f,
      display = "Minimum support",
      description = "The minimum support for an itemset to be identified as frequent. [0.0, 1.0]. Any pattern that appears " +
        "more than (minSupport * size-of-the-dataset) times will be output in the frequent itemsets. Default: 0.3",
      possible_values_description = "It can be any double value in range [0.0,1.0]",
      comma_separated_multiple_values = false,
      possible_values = None)

    val min_confidence = Parameter(AlgorithmParameterName.MIN_CONFIDENCE, DataType.DOUBLE, 0.8f,
      display = "Minimum confidence",
      description = "The minimum confidence for generating Association Rule. Confidence is an indication of how often an " +
        "association rule has been found to be true. minConfidence will not affect the mining for frequent itemsets, " +
        "but will affect the association rules generation. Default: 0.8",
      possible_values_description = "It can be any double value in range [0.0,1.0]",
      comma_separated_multiple_values = false,
      possible_values = None)

    val parametersForAllAlgorithms = Seq(num_folds, max_parallelism, metric)
    val parametersForAllClassificationAlgorithms = parametersForAllAlgorithms ++ Seq(handle_invalid, imputation_strategy)

    Seq(
      Algorithm(AlgorithmName.ARL_FPGROWTH, Some("FP Growth"), parametersForAllAlgorithms ++ Seq(min_support, min_confidence)),
      Algorithm(AlgorithmName.CLASSIFICATION_SVM, Some("Support Vector Machine (SVM)"), parametersForAllClassificationAlgorithms ++ Seq(max_iter)),
      Algorithm(AlgorithmName.CLASSIFICATION_LOGISTIC_REGRESSION, Some("Logistic Regression"), parametersForAllClassificationAlgorithms ++ Seq(threshold, max_iter, reg_param, elasticnet_param)),
      Algorithm(AlgorithmName.CLASSIFICATION_DECISION_TREE, Some("Decision Trees"), parametersForAllClassificationAlgorithms ++ Seq(max_depth, min_info_gain, impurity)),
      Algorithm(AlgorithmName.CLASSIFICATION_RANDOM_FOREST, Some("Random Forest"), parametersForAllClassificationAlgorithms ++ Seq(max_depth, min_info_gain, impurity, num_trees, feature_subset_strategy)),
      Algorithm(AlgorithmName.CLASSIFICATION_GBT, Some("Gradient Boosted Trees"), parametersForAllClassificationAlgorithms ++ Seq(max_iter, max_depth, min_info_gain, feature_subset_strategy)),
      // Algorithm(AlgorithmName.CLASSIFICATION_NAIVE_BAYES, Some("Naive Bayes"), parametersForAllAlgorithms), // Not working with existing BinaryClassificationEvaluator mechanism in Agents, hence removed from the list
      Algorithm(AlgorithmName.REGRESSION_LINEAR, Some("Linear Regression"), parametersForAllAlgorithms ++ Seq(threshold, max_iter, reg_param, elasticnet_param)),
      Algorithm(AlgorithmName.REGRESSION_DECISION_TREE, Some("Decision Trees"), parametersForAllAlgorithms ++ Seq(threshold, max_iter, reg_param, elasticnet_param)),
      Algorithm(AlgorithmName.REGRESSION_RANDOM_FOREST, Some("Random Forest"), parametersForAllAlgorithms ++ Seq(threshold, max_iter, reg_param, elasticnet_param)),
      Algorithm(AlgorithmName.REGRESSION_GBT, Some("Gradient Boosted Trees"), parametersForAllAlgorithms ++ Seq(threshold, max_iter, reg_param, elasticnet_param))
    )
  }

}
