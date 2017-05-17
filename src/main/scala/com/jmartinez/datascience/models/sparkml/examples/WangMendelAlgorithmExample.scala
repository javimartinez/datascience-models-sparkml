/*
 * Copyright 2016 jmartinez
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jmartinez.datascience.models.sparkml.examples

import com.jmartinez.datascience.models.sparkml.keelReader.KeelReader._
import com.jmartinez.datascience.models.sparkml.models.WangMendelAlgorithm
import org.apache.log4j.{ Level, Logger }

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{ StringIndexer, VectorAssembler }
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.{ DataFrame, SparkSession }

object WangMendelAlgorithmExample {

  def main(args: Array[String]) {

    // Disable INFO Log

    val logger = Logger.getLogger("my logger")

    logger.setLevel(Level.INFO)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark =
      SparkSession.builder
        .appName("WangMendelAlgorithm")
        //                .master("local[4]")
        .getOrCreate()

    logger.info("Wang&Mendel pipeline starts")

    val trainData =
      //            spark.keelFile("/Users/Javi/development/data/poker.dat")
      spark.keelFile("/workspace/data/poker-5-1tra.dat",10)

    val testData =
      spark.keelFile("/workspace/data/poker-5-1tst.dat",10)

    val assembler =
      new VectorAssembler().setInputCols(trainData.columns.dropRight(1)).setOutputCol("features")

    val stringIndexer = new StringIndexer().setInputCol("Class").setOutputCol("idx_Class")

    val pipelineToTransform = new Pipeline().setStages(Array(assembler, stringIndexer))

    val tranformedTrainData: DataFrame =
      pipelineToTransform.fit(trainData).transform(trainData)

    val transformedTestData: DataFrame = pipelineToTransform.fit(testData).transform(testData)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("idx_Class")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val wangMendelAlgorithm =
      new WangMendelAlgorithm()
        .setLabelCol("idx_Class")
        .setPredictionCol("prediction")
        .setNumFuzzyRegions(3)

    val result = wangMendelAlgorithm.fit(tranformedTrainData).transform(tranformedTrainData)

    //cross validation
    //    val paramGrid = new ParamGridBuilder()
    //      .addGrid(wangMendelAlgorithm.numFuzzyRegions, Array(1, 2, 3))
    //      .build()

    //    val cv: CrossValidator = new CrossValidator()
    //      .setEstimator(new Pipeline().setStages(Array(wangMendelAlgorithm)))
    //      .setEvaluator(evaluator)
    //      .setEstimatorParamMaps(paramGrid)
    //      .setNumFolds(2)
    //
    //
    //    val betterModel: CrossValidatorModel = cv.fit(tranformedTrainData)
    //
    //    betterModel.avgMetrics.foreach(println)

    //    val result =
    //      new Pipeline().setStages(Array(wangMendelAlgorithm)).fit(trainData)
    //        .transform(testData)

    //    println(result.count())

    //

    val accuracy = evaluator.evaluate(result)
    println(s"The accuracy is: ${accuracy * 100}")
    println(s"The simple error is: ${(1 - accuracy) * 100} ")

    //    evaluateRegressionModel(result, "idx_Class", "prediction")

    println("Spark job finished")
  }

  private def evaluateRegressionModel(
      data: DataFrame,
      labelColName: String,
      predictionLabel: String
  ): Unit = {
    val predictions = data.select(predictionLabel).rdd.map(_.getDouble(0))
    val labels      = data.select(labelColName).rdd.map(_.getDouble(0))
    val RMSE        = new RegressionMetrics(predictions.zip(labels)).meanSquaredError
    println(s"Root mean squared error (RMSE): $RMSE")
  }

  private def splitDataInToTrainAndTest(data: DataFrame): (DataFrame, DataFrame) = {
    val splits = data.randomSplit(Array(0.6, 0.4), 12345)

    (splits(0), splits(1))
  }

}
