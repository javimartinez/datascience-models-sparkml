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
import org.apache.spark.ml.tuning.{ CrossValidator, CrossValidatorModel, ParamGridBuilder }
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.{ DataFrame, SparkSession }

object WangMendelAlgorithmExample {

  def main(args: Array[String]) {

    // Disable INFO Log
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark =
      SparkSession.builder.appName("WangMendelAlgorithm").master("local[4]").getOrCreate()

    //    val data = spark.keelFile(args(0))

    val data =
      spark.keelFile("/Users/Javi/development/data/poker.dat")

    val assembler =
      new VectorAssembler().setInputCols(data.columns.dropRight(1)).setOutputCol("features")

    val stringIndexer = new StringIndexer().setInputCol("Class").setOutputCol("idx_Class")

    val tranformedData: DataFrame =
      new Pipeline().setStages(Array(assembler, stringIndexer)).fit(data).transform(data)

    tranformedData.show()

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("idx_Class")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val wangMendelAlgorithm =
      new WangMendelAlgorithm()
        .setLabelCol("idx_Class")
        .setPredictionCol("prediction")
        .setNumFuzzyRegions(5)

    val paramGrid = new ParamGridBuilder()
      .addGrid(wangMendelAlgorithm.numFuzzyRegions, Array(1, 2, 5, 6, 19))
      .build()

//    val indexToString = new IndexToString()

    val cv: CrossValidator = new CrossValidator()
      .setEstimator(new Pipeline().setStages(Array(wangMendelAlgorithm)))
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)

    val betterModel: CrossValidatorModel = cv.fit(tranformedData)
    val result                           = betterModel.transform(tranformedData)


    val accuracy = evaluator.evaluate(result)
    println(s"The accuracy is: ${accuracy * 100}")
    println(s"The simple error is: ${(1 - accuracy) * 100} ")

    evaluateRegressionModel(result, "idx_Class", "prediction")

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
    println(s"  Root mean squared error (RMSE): $RMSE")
  }
  //    val predictedData = new Pipeline()
  //      .setStages(Array(wangMendelAlgorithm))
  //      .fit(tranformedData)
  //      .transform(tranformedData)
}
