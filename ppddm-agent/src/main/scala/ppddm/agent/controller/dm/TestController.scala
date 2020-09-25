package ppddm.agent.controller.dm

import akka.Done
import com.typesafe.scalalogging.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession
import ppddm.agent.Agent
import ppddm.agent.controller.prepare.DataStoreManager
import ppddm.core.ai.RegressionHandler
import ppddm.core.rest.model.AlgorithmExecutionRequest

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object TestController {

  private val logger: Logger = Logger(this.getClass)
  private val sparkSession: SparkSession = Agent.dataMiningEngine.sparkSession

  import sparkSession.implicits._

  def startAlgorithmExecution(algorithmExecutionRequest: AlgorithmExecutionRequest): Future[Done] = {
    logger.debug("Algorithm execution request received.")

    val df = DataStoreManager.getDF(DataStoreManager.getDatasetPath(algorithmExecutionRequest.dataset_id))

    if(df.isDefined) {
      df.get.show(false)

      // df.get.describe("CKD").show() // just some statistics

      // VectorAssembler to add features column
      val assembler = new VectorAssembler()
        .setInputCols(Array("CKD", "Heartfailure", "Numberofprescribeddrugs")) // columns that need to added to feature column
        .setOutputCol("features")

      val assemblerOutput = assembler.transform(df.get)

      val indexer = new StringIndexer()
        .setInputCol("Hypertension")
        .setOutputCol("label")
      val indexerOutput = indexer.fit(assemblerOutput).transform(assemblerOutput)

      // Split data into training (70%) and test (30%).
      val Array(pipelineTrainingData, pipelineTestingData) = indexerOutput.randomSplit(Array(0.7, 0.3), seed = 11L)

      val labeled = pipelineTrainingData.rdd.map(row => LabeledPoint(
        row.getAs[Double]("label"),
        org.apache.spark.mllib.linalg.Vectors.fromML(row.getAs[org.apache.spark.ml.linalg.Vector]("features"))
      ))

      val a = new LogisticRegressionWithLBFGS()
        .setNumClasses(10)
      val model = a.run(labeled, Vectors.dense(-40.107195189130124,-41.91543101145674,4.465742804205058,-16.98499447738941,-16.56786565759739,0.1519516191987702,-16.98499447738941,-16.56786565759739,0.1519516191987702,-16.98499447738941,-16.56786565759739,0.1519516191987702,-16.98499447738941,-16.56786565759739,0.1519516191987702,-16.98499447738941,-16.56786565759739,0.1519516191987702,-16.98499447738941,-16.56786565759739,0.1519516191987702,-16.98499447738941,-16.56786565759739,0.1519516191987702,-16.98499447738941,-16.56786565759739,0.1519516191987702))

      println(s"Weights: \n${model.weights}")

      val predictionAndLabels = pipelineTestingData.rdd.map { row =>
        val label = row.getAs[Double]("label")
        val features = org.apache.spark.mllib.linalg.Vectors.fromML(row.getAs[org.apache.spark.ml.linalg.Vector]("features"))
        val prediction = model.predict(features)
        (prediction, label)
      }

      // Get evaluation metrics.
      val metrics = new MulticlassMetrics(predictionAndLabels)
      val accuracy = metrics.accuracy
      val falsePositiveRate = metrics.weightedFalsePositiveRate
      val truePositiveRate = metrics.weightedTruePositiveRate
      val fMeasure = metrics.weightedFMeasure
      val precision = metrics.weightedPrecision
      val recall = metrics.weightedRecall
      println(s"Accuracy = $accuracy")
      println(s"False positive rate = $falsePositiveRate")
      println(s"True positive rate = $truePositiveRate")
      println(s"fMeasure = $fMeasure")
      println(s"Precision = $precision")
      println(s"Recall = $recall")

    } else {
      Future {
        logger.error(s"Dataset with id: ${algorithmExecutionRequest.dataset_id} does not exist.")
        Done // TODO throw an exception?
      }
    }

    null
  }

  def startAlgorithmExecution2(algorithmExecutionRequest: AlgorithmExecutionRequest): Future[Done] = {
    logger.debug("Algorithm execution request received.")

    val df = DataStoreManager.getDF(DataStoreManager.getDatasetPath(algorithmExecutionRequest.dataset_id))

    if(df.isDefined) {
      df.get.show(false)

      // df.get.describe("CKD").show() // just some statistics

      // VectorAssembler to add features column
      val assembler = new VectorAssembler()
        .setInputCols(Array("CKD", "Heartfailure", "Hypertension")) // columns that need to added to feature column
        .setOutputCol("features")

      val indexer = new StringIndexer()
        .setInputCol("Numberofprescribeddrugs")
        .setOutputCol("label")

      // Split data into training (70%) and test (30%).
      val Array(pipelineTrainingData, pipelineTestingData) = df.get.randomSplit(Array(0.7, 0.3), seed = 11L)
      /*
      val splits = output2.randomSplit(Array(0.6, 0.4), seed = 11L)
      val training = splits(0).cache()
      val test = splits(1)
       */

      // Train logistic regression model with training data set
      val logisticRegression = new LogisticRegression()
        .setMaxIter(100)
        .setRegParam(0.02)


      logisticRegression.setUpperBoundsOnIntercepts(org.apache.spark.ml.linalg.Vectors.dense(2.348905201577959,-0.5470803985698622,-0.17121941326613174,-1.630605389741965))

      // VectorAssembler and StringIndexer are transformers
      // LogisticRegression is the estimator
      val stages = Array(assembler, indexer, logisticRegression)

      // build pipeline
      val pipeline = new Pipeline().setStages(stages)
      val pipelineModel = pipeline.fit(pipelineTrainingData)

      // Print the coefficients and intercept for multinomial logistic regression
      // Get fitted logistic regression model
      val lrModel = pipelineModel.stages.last.asInstanceOf[LogisticRegressionModel]
      println(s"Coefficients: \n${lrModel.coefficientMatrix}")
      println(s"Intercepts: ${lrModel.interceptVector}")

      // test model with test data
      val pipelinePredictionDf = pipelineModel.transform(pipelineTestingData)
      pipelinePredictionDf.show(false)

      val predictionLabelsRDD = pipelinePredictionDf.select("prediction", "label").rdd.map(r => (r.getDouble(0), r.getDouble(1)))

      // evaluate model with area under ROC
      /* val evaluator = new BinaryClassificationEvaluator()
         .setLabelCol("label")
         .setMetricName("areaUnderROC")

       // measure the accuracy of pipeline model
       val pipelineAccuracy = evaluator.evaluate(pipelinePredictionDf)
       println(pipelineAccuracy)*/


      // Get evaluation metrics.
      /*val metrics = new MulticlassMetrics(predictionLabelsRDD)
      val accuracy = metrics.accuracy
      val falsePositiveRate = metrics.weightedFalsePositiveRate
      val truePositiveRate = metrics.weightedTruePositiveRate
      val fMeasure = metrics.weightedFMeasure
      val precision = metrics.weightedPrecision
      val recall = metrics.weightedRecall
      println(s"Accuracy = $accuracy")
      println(s"False positive rate = $falsePositiveRate")
      println(s"True positive rate = $truePositiveRate")
      println(s"fMeasure = $fMeasure")
      println(s"Precision = $precision")
      println(s"Recall = $recall")*/

      // Instantiate metrics object
      val metrics = new BinaryClassificationMetrics(predictionLabelsRDD)

      // Precision by threshold
      val precision = metrics.precisionByThreshold
      precision.collect.foreach { case (t, p) =>
        println(s"Threshold: $t, Precision: $p")
      }

      // Recall by threshold
      val recall = metrics.recallByThreshold
      recall.collect.foreach { case (t, r) =>
        println(s"Threshold: $t, Recall: $r")
      }

      // Precision-Recall Curve
      val PRC = metrics.pr

      // F-measure
      val f1Score = metrics.fMeasureByThreshold
      f1Score.collect.foreach { case (t, f) =>
        println(s"Threshold: $t, F-score: $f, Beta = 1")
      }

      val beta = 0.5
      val fScore = metrics.fMeasureByThreshold(beta)
      fScore.collect.foreach { case (t, f) =>
        println(s"Threshold: $t, F-score: $f, Beta = 0.5")
      }

      // AUPRC
      val auPRC = metrics.areaUnderPR
      println(s"Area under precision-recall curve = $auPRC")

      // Compute thresholds used in ROC and PR curves
      val thresholds = precision.map(_._1)

      // ROC Curve
      val roc = metrics.roc

      // AUROC
      val auROC = metrics.areaUnderROC
      println(s"Area under ROC = $auROC")

    } else {
      Future {
        logger.error(s"Dataset with id: ${algorithmExecutionRequest.dataset_id} does not exist.")
        Done // TODO throw an exception?
      }
    }

    null
  }

  def testRegression(): Seq[String] = {


    /* import org.apache.spark.ml.feature.StringIndexer
     import org.apache.spark.ml.feature.OneHotEncoder

     val df = Agent.dataMiningEngine.sparkSession.createDataFrame(
       Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
     ).toDF("id", "category")

     // Split the data into training and test sets (30% held out for testing).
     val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))

     val indexer = new StringIndexer()
       .setInputCol("category")
       .setOutputCol("categoryIndex")

     val encoder = new OneHotEncoder()
       .setInputCols(Array("categoryIndex"))
       .setOutputCols(Array("categoryVec"))

     val lr = new LogisticRegression()
       .setLabelCol("id")
       .setFeaturesCol("categoryVec")

     // We may set parameters using setter methods.
     lr.setMaxIter(10)
       .setRegParam(0.01)

     val pipeline = new Pipeline()
       .setStages(Array(indexer, encoder, lr))

     val model = pipeline.fit(trainingData)
     val predictedDF = model.transform(testData)

     predictedDF.show()*/



    new RegressionHandler(Agent.dataMiningEngine.sparkSession).test()
  }

  def basicStatistics(): Unit = {
    import org.apache.spark.mllib.linalg.Vectors
    import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

    val sc = Agent.dataMiningEngine.sparkSession.sparkContext

    val observations = sc.parallelize(
      Seq(
        Vectors.dense(1.0, 10.0, 100.0),
        Vectors.dense(2.0, 20.0, 200.0),
        Vectors.dense(3.0, 30.0, 300.0)
      )
    )

    // Compute column summary statistics.
    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
    println("Mean: " + summary.mean)  // a dense vector containing the mean value for each column
    println("Variance: " + summary.variance)  // column-wise variance
    println("Min: " + summary.min)
    println("Max: " + summary.max)
    println("NonZeros: " + summary.numNonzeros)  // number of nonzeros in each column
  }

  def correlations(): Unit = {
    import org.apache.spark.mllib.linalg._
    import org.apache.spark.mllib.stat.Statistics
    import org.apache.spark.rdd.RDD

    val sc = Agent.dataMiningEngine.sparkSession.sparkContext

    val seriesX: RDD[Double] = sc.parallelize(Array(1, 2, 3, 3, 5))  // a series
    // must have the same number of partitions and cardinality as seriesX
    val seriesY: RDD[Double] = sc.parallelize(Array(11, 22, 33, 33, 555))

    // compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a
    // method is not specified, Pearson's method will be used by default.
    val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")
    println(s"Correlation is: $correlation")

    val data: RDD[Vector] = sc.parallelize(
      Seq(
        Vectors.dense(1.0, 10.0, 100.0, 4.0),
        Vectors.dense(2.0, 20.0, 200.0, 8.0),
        Vectors.dense(5.0, 33.0, 366.0, 11.0))
    )  // note that each Vector is a row and not a column

    // calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method
    // If a method is not specified, Pearson's method will be used by default.
    val correlMatrix: Matrix = Statistics.corr(data, "pearson")
    println(correlMatrix.toString)
  }

  def featureHasher(): Unit = {
    import org.apache.spark.ml.feature.FeatureHasher

    val dataset = Agent.dataMiningEngine.sparkSession.createDataFrame(Seq(
      (2.2, true, "1", "foo"),
      (3.3, false, "2", "bar"),
      (4.4, false, "3", "baz"),
      (5.5, false, "4", "foo")
    )).toDF("real", "bool", "stringNum", "string")

    val hasher = new FeatureHasher()
      .setInputCols("real", "bool", "stringNum", "string")
      .setOutputCol("features")

    val featurized = hasher.transform(dataset)
    featurized.show(false)
  }

  def pca(): Unit = {
    import org.apache.spark.ml.feature.PCA
    import org.apache.spark.ml.linalg.Vectors

    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )
    val df =  Agent.dataMiningEngine.sparkSession.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3)
      .fit(df)

    val result = pca.transform(df).select("pcaFeatures")
    result.show(false)
  }


  def stringIndexer(): Unit = {
    import org.apache.spark.ml.feature.StringIndexer

    val df = Agent.dataMiningEngine.sparkSession.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val indexed = indexer.fit(df).transform(df)
    indexed.show()
  }

  def oneHotEncoder(): Unit = {
    import org.apache.spark.ml.feature.OneHotEncoder

    val df = Agent.dataMiningEngine.sparkSession.createDataFrame(Seq(
      (0.0, 1.0),
      (1.0, 0.0),
      (2.0, 1.0),
      (0.0, 2.0),
      (0.0, 1.0),
      (2.0, 0.0)
    )).toDF("categoryIndex1", "categoryIndex2")

    val encoder = new OneHotEncoder()
      .setInputCols(Array("categoryIndex1", "categoryIndex2"))
      .setOutputCols(Array("categoryVec1", "categoryVec2"))
    val model = encoder.fit(df)

    val encoded = model.transform(df)
    encoded.show()
  }

  def normalizer(): Unit = {
    import org.apache.spark.ml.feature.Normalizer
    import org.apache.spark.ml.linalg.Vectors

    val dataFrame = Agent.dataMiningEngine.sparkSession.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.5, -1.0)),
      (1, Vectors.dense(2.0, 1.0, 1.0)),
      (2, Vectors.dense(4.0, 10.0, 2.0))
    )).toDF("id", "features")

    // Normalize each Vector using $L^1$ norm.
    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0)

    val l1NormData = normalizer.transform(dataFrame)
    println("Normalized using L^1 norm")
    l1NormData.show()

    // Normalize each Vector using $L^\infty$ norm.
    val lInfNormData = normalizer.transform(dataFrame, normalizer.p -> Double.PositiveInfinity)
    println("Normalized using L^inf norm")
    lInfNormData.show()
  }

  def vectorAssembler(): Unit = {
    import org.apache.spark.ml.feature.VectorAssembler
    import org.apache.spark.ml.linalg.Vectors

    val dataset = Agent.dataMiningEngine.sparkSession.createDataFrame(
      Seq((0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0))
    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")

    val assembler = new VectorAssembler()
      .setInputCols(Array("hour", "mobile", "userFeatures"))
      .setOutputCol("features")

    val output = assembler.transform(dataset)
    println("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
    output.select("features", "clicked").show(false)
  }

  def imputer(): Unit = {
    import org.apache.spark.ml.feature.Imputer

    val df = Agent.dataMiningEngine.sparkSession.createDataFrame(Seq(
      (1.0, Double.NaN),
      (2.0, Double.NaN),
      (Double.NaN, 3.0),
      (4.0, 4.0),
      (5.0, 5.0)
    )).toDF("a", "b")

    val imputer = new Imputer()
      .setInputCols(Array("a", "b"))
      .setOutputCols(Array("out_a", "out_b"))

    val model = imputer.fit(df)
    model.transform(df).show()
  }

  def logisticRegression1(): Unit = {
    import org.apache.spark.ml.classification.LogisticRegression

    // Load training data
    val training = Agent.dataMiningEngine.sparkSession.read.format("libsvm").load("ppddm-agent/src/main/resources/data.txt")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // We can also use the multinomial family for binary classification
    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFamily("multinomial")

    val mlrModel = mlr.fit(training)

    // Print the coefficients and intercepts for logistic regression with multinomial family
    println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
    println(s"Multinomial intercepts: ${mlrModel.interceptVector}")

    // Extract the summary from the returned LogisticRegressionModel instance trained in the earlier
    // example
    val trainingSummary = lrModel.binarySummary

    val accuracy = trainingSummary.accuracy
    val falsePositiveRate = trainingSummary.weightedFalsePositiveRate
    val truePositiveRate = trainingSummary.weightedTruePositiveRate
    val fMeasure = trainingSummary.weightedFMeasure
    val precision = trainingSummary.weightedPrecision
    val recall = trainingSummary.weightedRecall
    println(s"Accuracy: $accuracy\nFPR: $falsePositiveRate\nTPR: $truePositiveRate\n" +
      s"F-measure: $fMeasure\nPrecision: $precision\nRecall: $recall")


  }

  def logisticRegression2(): Unit = {
    import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
    import org.apache.spark.mllib.evaluation.MulticlassMetrics
    import org.apache.spark.mllib.regression.LabeledPoint
    import org.apache.spark.mllib.util.MLUtils

    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(Agent.dataMiningEngine.sparkSession.sparkContext,
      "ppddm-agent/src/main/resources/data.txt")

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(training)

    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.accuracy
    val falsePositiveRate = metrics.weightedFalsePositiveRate
    val truePositiveRate = metrics.weightedTruePositiveRate
    val fMeasure = metrics.weightedFMeasure
    val precision = metrics.weightedPrecision
    val recall = metrics.weightedRecall
    println(s"Accuracy = $accuracy")
    println(s"False positive rate = $falsePositiveRate")
    println(s"True positive rate = $truePositiveRate")
    println(s"fMeasure = $fMeasure")
    println(s"Precision = $precision")
    println(s"Recall = $recall")

    // Save and load model
    /*model.save(sc, "target/tmp/scalaLogisticRegressionWithLBFGSModel")
    val sameModel = LogisticRegressionModel.load(sc,
      "target/tmp/scalaLogisticRegressionWithLBFGSModel")*/
  }

}
