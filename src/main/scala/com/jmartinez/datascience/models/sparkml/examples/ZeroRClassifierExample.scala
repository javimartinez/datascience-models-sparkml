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

import java.util.concurrent.TimeUnit._

import com.jmartinez.datascience.models.sparkml.models.ZeroRClassifier
import org.apache.log4j.{ Level, Logger }

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{ StringIndexer, VectorAssembler, VectorIndexer }
import org.apache.spark.sql.SparkSession

object ZeroRExample {

  def main(args: Array[String]) {

    // Disable INFO Log
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark =
      SparkSession.builder.appName("ZeroRClassifierExample").getOrCreate()

    // $example on$

    // Crates a DataFrame
    val dataDF =
      spark.read
        .format("com.databricks.spark.csv")
        .option("header", "true")      // Use first line of all files as header
        .option("inferSchema", "true") // Automatically infer data types
        .load("/Users/Javi/Development/data/iris.csv")

    // Transformer
    val assembler =
      new VectorAssembler().setInputCols(dataDF.columns.dropRight(1)).setOutputCol("features")

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer =
      new StringIndexer().setInputCol("Species").setOutputCol("label")

    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous

    val prepareDataPipeline =
      new Pipeline().setStages(Array(assembler, labelIndexer, featureIndexer))

    val indexedDataset = prepareDataPipeline.fit(dataDF).transform(dataDF)

    val zeroR =
      new ZeroRClassifier()
        .setLabelCol("label")
        .setFeaturesCol("indexedFeatures")
        .setPredictionCol("predictedLabel")

    val (trainingDuration, zeroRModel) = time(zeroR.fit(indexedDataset))

    val (predictionDuration, prediction) = time(zeroRModel.transform(indexedDataset))

    val predictionsAndLabels = prediction.select("predictedLabel", "label")

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("predictedLabel")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictionsAndLabels)

    println(s" Training Time ${ trainingDuration } milliseconds\n")

    println(s" Prediction Time ${ predictionDuration } milliseconds\n")

    println(s"The accuracy is : ${ accuracy * 100 } % ")

    spark.stop()

  }

  private def time[R](block: => R): (Long, R) = {
    val t0     = System.nanoTime()
    val result = block // call-by-name
    val t1     = System.nanoTime()
    (NANOSECONDS.toMillis(t1 - t0), result)
  }
}
