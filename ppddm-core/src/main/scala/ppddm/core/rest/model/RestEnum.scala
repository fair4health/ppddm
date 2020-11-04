package ppddm.core.rest.model

object ProjectType extends Enumeration {
  type ProjectType = String
  val PREDICTION = "prediction"
  val ASSOCIATION = "association"
}

object VariableDataType extends Enumeration {
  type VariableDataType = String
  val CATEGORICAL = "categorical"
  val NUMERIC = "numeric"
}

object VariableType extends Enumeration {
  type VariableType = String
  val INDEPENDENT = "independent"
  val DEPENDENT = "dependent"
}

object SelectionStatus extends Enumeration {
  type SelectionStatus = String
  val SELECTED = "selected"
  val DISCARDED = "discarded"
}

/**
 * ExecutionState is being used by Dataset and DatasetSource classes of the rest model.
 *
 * Dataset: When a Dataset is created, it will invoke Agents to prepare their data. The ExecutionState of the Dataset
 * will be EXECUTING at this point. Once all Agents prepare their data, th ExecutionState of the Dataset will be
 * READY since the user now will select/discard the data sources of the Dataset. After this selection,
 * the ExecutionState will be FINAL.
 *
 * DatasetSource: When an Agent is invoked to prepare its data, the associated DatasetSource's ExecutionState will be
 * EXECUTING. When the data is ready at the Agent, then the ExecutionState will be FINAL. Hence, DatasetSource uses only
 * two of the available ExecutionState enumeration values.
 *
 */
object ExecutionState extends Enumeration {
  type ExecutionState = String
  val EXECUTING = "executing"
  val READY = "ready"
  val FINAL = "final"
}

/**
 * DataMiningState is being used by DataMiningModel and WeakModel classes of the rest model.
 *
 * DataMiningModel: TODO
 *
 * WeakModel: TODO
 *
 */
object DataMiningState extends Enumeration {
  type DataMiningState = String
  val TRAINING = "training"
  val VALIDATING = "validating"
  val TESTING = "testing"
  val READY = "ready"
  val FINAL = "final"
}

object FHIRPathExpressionPrefix extends Enumeration {
  type FHIRPathExpressionPrefix = String
  val AGGREGATION = "aggr:"
  val AGGREGATION_EXISTENCE = "aggr:exists:"
  val VALUE = "value:"
  val SATISFY = "satisfy:"
}

object DataType extends Enumeration {
  type DataType = String
  val INTEGER = "integer"
  val DOUBLE = "double"
  val STRING = "string"
}

object AlgorithmName extends Enumeration {
  type AlgorithmName = String
  val ARL_BRUTE_FORCE = "arl_brute_force"
  val ARL_APRIORI = "arl_apriori"
  val ARL_ECLAT = "arl_eclat"
  val ARL_FPGROWTH = "arl_fpgrowth"
  val CLUSTERING_KMEANS = "clustering_kmeans"
  val CLUSTERING_HIERARCHICAL = "clustering_hierarchical"
  val CLUSTERING_GAUSSIAN_MIXTURE = "clustering_gaussian_mixture"
  val CLUSTERING_POWER_ITERATION = "clustering_power_iteration"
  val CLUSTERING_BISECTING_KMEANS = "clustering_bisecting_kmeans"
  val CLUSTERING_STREAMING_KMEANS = "clustering_streaming_kmeans"
  val CLASSIFICATION_SVM = "classification_svm"
  val CLASSIFICATION_LOGISTIC_REGRESSION = "classification_logistic_regression"
  val CLASSIFICATION_DECISION_TREE = "classification_decision_tree"
  val CLASSIFICATION_RANDOM_FOREST = "classification_random_forest"
  val CLASSIFICATION_GBT = "classification_gbt"
  val CLASSIFICATION_NAIVE_BAYES = "classification_naive_bayes"
  val CLASSIFICATION_KNN = "classification_knn"
  val REGRESSION_LLS = "regression_lls"
  val REGRESSION_LASSO = "regression_lasso"
  val REGRESSION_RIDGE = "regression_ridge"
  val REGRESSION_DECISION_TREE = "regression_decision_tree"
  val REGRESSION_RANDOM_FOREST = "regression_random_forest"
  val REGRESSION_GBT = "regression_gbt"
}

object AlgorithmParameterName extends Enumeration {
  type AlgorithmParameterName = String
  val NUMBER_OF_FOLDS = "num_folds" // Value of k in k-fold Cross validation
  val MAX_PARALLELISM = "max_parallelism" // The maximum level of parallelism to evaluate models in parallel. 1 means serial evaluation

  // Logistic Regression
  val THRESHOLD = "threshold" // Threshold [Double]
  val MAX_ITER = "max_iter" // Maximum number of iterations [Integer]
  val REG_PARAM = "reg_param" // Regularization parameter [Double]
  val ELASTIC_NET_PARAM = "elasticnet_param" // ElasticNet mixing parameter [Double]


  // Decision Tree + Random Forest
  val MAX_DEPTH = "max_depth" // Maximum depth of a tree. Deeper trees are more expressive (potentially allowing higher accuracy), but they are also more costly to train and are more likely to overfit.
  val MIN_INFO_GAIN = "min_info_gain" // For a node to be split further, the split must improve at least this much (in terms of information gain).
  val MAX_BINS = "max_bins" // TODO Consider using this if you will not perform categorical handling or feature scaling for decision tree
  val IMPURITY = "impurity" // The node impurity is a measure of the homogeneity of the labels at the node. Values can be "gini" or "entropy"

  // RandomForest
  val NUM_TREES = "num_trees" // Number of trees in the forest. Increasing the number of trees will decrease the variance in predictions, improving the model’s test-time accuracy
  val FEATURE_SUBSET_STRATEGY = "feature_subset_strategy" // Number of features to use as candidates for splitting at each tree node. Default is "auto"
}

object AlgorithmStatisticsName extends Enumeration {
  type AlgorithmStatisticsName = String
  val TRUE_POSITIVE = "true_positive"
  val TRUE_NEGATIVE = "true_negative"
  val FALSE_POSITIVE = "false_positive"
  val FALSE_NEGATIVE = "false_negative"
  val TOTAL = "total"
  val ACCURACY = "accuracy"
  val PRECISION = "precision"
  val RECALL = "recall"
  val FPR = "false_positive_rate"
  val TPR = "true_positive_rate"
  val F_MEASURE = "f_measure"
  val AUROC = "area_under_roc"
  val AUPR = "area_under_pr"
}
