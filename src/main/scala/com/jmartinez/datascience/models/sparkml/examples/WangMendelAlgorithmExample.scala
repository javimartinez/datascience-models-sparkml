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

import org.apache.spark.ml.{ Pipeline, Transformer }
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{ DataFrame, Row, SparkSession }

object WangMendelAlgorithmExample {

  def main(args: Array[String]) {

    // Disable INFO Log
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark =
      SparkSession.builder.appName("WangMendelAlgorithm").master("local[4]").getOrCreate()

    //    val data = spark.keelFile(args(0))

    val data =
      spark.keelFile("/Users/Javi/development/datascience-models-sparkml/data/machineCPU.dat")

    val assembler =
      new VectorAssembler().setInputCols(data.columns.dropRight(1)).setOutputCol("features")

    val wangMendelAlgorithm =
      new WangMendelAlgorithm()
        .setLabelCol("PRP")
        .setPredictionCol("prediction")
        .setNumFuzzyRegions(5)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("PRP")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val tranformedData: DataFrame =
      new Pipeline().setStages(Array(assembler)).fit(data).transform(data)

    val predictedData = new Pipeline()
      .setStages(Array(wangMendelAlgorithm))
      .fit(tranformedData)
      .transform(tranformedData)

    predictedData.show(100)

    val accuracy = evaluator.evaluate(predictedData)
    println(s"The accuracy is: ${accuracy * 100}")
    println(s"The simple error is: ${(1 - accuracy) * 100} ")
    evaluateRegressionModel(predictedData, "PRP", "prediction")

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

}
