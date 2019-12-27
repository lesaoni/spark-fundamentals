package com.luxoft.training.eas017.day3.ml

import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}


object ClassificationExample extends App {

  val spark = SparkSession
        .builder
        .master("local[*]")
        .appName("Graph Frame Example")
        .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.functions._
  import org.apache.spark.ml._
  import org.apache.spark.ml.feature._
  import org.apache.spark.ml.classification._

  val docs = spark.sparkContext
    .wholeTextFiles("hdfs:///user/centos/eas-017/ml/20_newsgroup/*")
    .map(s => (s._1.replace("hdfs:///user/centos/eas-017/ml/20_newsgroup/", ""), s._2))
    .toDF("topic", "text")


  def preprocess(df : DataFrame) = df
    .withColumn("label", col("topic").like("sci.%").cast("double"))

  val Array(training, test) = preprocess(docs).randomSplit(Array(0.8, 0.2), seed = 12345)

  training.groupBy("label").agg(count("*").as("count")).show

  val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")

  val hashingTF = new HashingTF().setNumFeatures(5000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")

  val lr = new LogisticRegression().setMaxIter(20).setRegParam(0.01)

  val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))

  val model = pipeline.fit(training)

  val evaluator = new BinaryClassificationEvaluator()
    .setMetricName("areaUnderROC")

  def roc(model: Transformer, df: DataFrame) =
    evaluator.evaluate(model.transform(df), ParamMap.empty)

  roc(model, training)

  roc(model, test)

  val paramGrid = new ParamGridBuilder()
    .addGrid(hashingTF.numFeatures, Array(1000, 10000))
    .addGrid(lr.regParam, Array(0.005, 0.1, 0.2))
    .build

  val cv = new CrossValidator()
    .setEstimator(pipeline)
    .setEvaluator(evaluator)
    .setEstimatorParamMaps(paramGrid)
    .setNumFolds(3)

  val cvModel = cv.fit(training)

  roc(cvModel, test)

}
