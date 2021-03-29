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

object CategoricalEncodingType extends Enumeration {
  type CategoricalEncodingType = String
  val DUMMY = "introduce_dummy"
  val NUMERIC = "convert_to_numeric"
}

object MissingDataOperationType extends Enumeration {
  type MissingDataOperationType = String
  val SET_MIN = "set_min"
  val SET_MAX = "set_max"
  val SET_AVERAGE = "set_average"
  val SET_MEDIAN = "set_median"
  val SET_SPECIFIC = "set_specific"
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
 * the ExecutionState will be FINAL. If one of the DatasetSources of the Dataset receives error during dataset preparation,
 * then the ExecutionState of the dataset will be set to ERROR.
 *
 * DatasetSource: When an Agent is invoked to prepare its data, the associated DatasetSource's ExecutionState will be
 * EXECUTING. When the data is ready at the Agent, then the ExecutionState will be FINAL. Hence, DatasetSource uses only
 * two of the available ExecutionState enumeration values. If data preparation gives an error on the associated Agent of
 * the DatasetSource then the ExecutionState will be set to ERROR.
 *
 */
object ExecutionState extends Enumeration {
  type ExecutionState = String
  val EXECUTING = "executing"
  val READY = "ready"
  val FINAL = "final"
  val ERROR = "error"
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

  val CALCULATING_FREQUENCY_ARL = "calculating_frequency_arl"
  val EXECUTING_ARL = "executing_arl"

  val READY = "ready"
  val FINAL = "final"
  val ERROR = "error"
}

object FHIRPathExpressionPrefix extends Enumeration {
  type FHIRPathExpressionPrefix = String
  val AGGREGATION = "aggr:"
  val AGGREGATION_EXISTENCE = "aggr:exists:"
  val VALUE = "value:"
  val SATISFY = "satisfy:"
  val VALUE_READMISSION = "value:readmission:"
  val VALUE_HOSPITALIZATION = "value:hospitalization:"
}

object DataType extends Enumeration {
  type DataType = String
  val INTEGER = "integer"
  val DOUBLE = "double"
  val STRING = "string"
}

object AlgorithmName extends Enumeration {
  type AlgorithmName = String
  val ARL_PREFIX_SPAN = "arl_prefix_span"
  val ARL_FPGROWTH = "arl_fpgrowth"
  val CLASSIFICATION_SVM = "classification_svm"
  val CLASSIFICATION_LOGISTIC_REGRESSION = "classification_logistic_regression"
  val CLASSIFICATION_DECISION_TREE = "classification_decision_tree"
  val CLASSIFICATION_RANDOM_FOREST = "classification_random_forest"
  val CLASSIFICATION_GBT = "classification_gbt"
  val CLASSIFICATION_NAIVE_BAYES = "classification_naive_bayes"
  val REGRESSION_LINEAR = "regression_linear"
  val REGRESSION_DECISION_TREE = "regression_decision_tree"
  val REGRESSION_RANDOM_FOREST = "regression_random_forest"
  val REGRESSION_GBT = "regression_gbt"
}

object AlgorithmParameterName extends Enumeration {
  type AlgorithmParameterName = String
  val NUMBER_OF_FOLDS = "num_folds" // Value of k in k-fold Cross validation
  val MAX_PARALLELISM = "max_parallelism" // The maximum level of parallelism to evaluate models in parallel. 1 means serial evaluation
  val METRIC = "metric" // Metric to use on Cross validation

  val THRESHOLD = "threshold" // Threshold (Double) [Logistic Regression]
  val MAX_ITER = "max_iter" // Maximum number of iterations (Integer) [Logistic Regression, GBT, SVM]
  val REG_PARAM = "reg_param" // Regularization parameter (Double) [Logistic Regression, SVM]
  val ELASTIC_NET_PARAM = "elasticnet_param" // ElasticNet mixing parameter (Double) [Logistic Regression]

  val MAX_DEPTH = "max_depth" // Maximum depth of a tree. Deeper trees are more expressive (potentially allowing higher accuracy), but they are also more costly to train and are more likely to overfit. [Decision Tree, Random Forest, GBT]
  val MIN_INFO_GAIN = "min_info_gain" // For a node to be split further, the split must improve at least this much (in terms of information gain). [Decision Tree, Random Forest, GBT]
  val MAX_BINS = "max_bins" // TODO Consider using this if you will not perform categorical handling or feature scaling for decision tree
  val IMPURITY = "impurity" // The node impurity is a measure of the homogeneity of the labels at the node. Values can be "gini" or "entropy" [Decision Tree, Random Forest]

  val NUM_TREES = "num_trees" // Number of trees in the forest. Increasing the number of trees will decrease the variance in predictions, improving the model’s test-time accuracy [Random Forest]
  val FEATURE_SUBSET_STRATEGY = "feature_subset_strategy" // Number of features to use as candidates for splitting at each tree node. Default is "auto" [Random Forest, GBT]

  val MIN_SUPPORT = "min_support" // The minimum support for an itemset to be identified as frequent [ARL]
  val MIN_CONFIDENCE = "min_confidence" // The minimum confidence for generating Association Rule. Confidence is an indication of how often an association rule has been found to be true. [ARL]
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
