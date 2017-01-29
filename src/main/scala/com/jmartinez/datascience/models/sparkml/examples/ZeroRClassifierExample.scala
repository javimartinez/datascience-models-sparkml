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

import com.jmartinez.datascience.models.sparkml.models.{ OneRClassifier, ZeroRClassifier }
import org.apache.log4j.{ Level, Logger }

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{ StringIndexer, VectorAssembler, VectorIndexer }
import org.apache.spark.sql.SparkSession
import com.jmartinez.datascience.models.sparkml.keelReader.KeelReader._

object ZeroRClassifierExample {

  def main(args: Array[String]) {

    // Disable INFO Log
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Creates a DataFrame

    val spark =
      SparkSession.builder.appName("ZeroRClassifierExample").getOrCreate()

    // Crates a DataFrame
    val dataDF = spark.keelFile(args(0))

    // Transformer

    val (stringIndexers, columns) = dataDF.columns.map { column =>
      val newColumn = s"idx_${column}"
      (new StringIndexer().setInputCol(column).setOutputCol(newColumn), newColumn)
    }.toList.unzip

    val assembler =
      new VectorAssembler().setInputCols(columns.dropRight(1).toArray).setOutputCol("features")

    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous

    val transformers = stringIndexers ::: List(assembler) ::: List(featureIndexer)

    val prepareDataPipeline =
      new Pipeline().setStages(transformers.toArray)

    val indexedDataset = prepareDataPipeline.fit(dataDF).transform(dataDF)

    val zeroR =
      new ZeroRClassifier()
        .setLabelCol("idx_Class")
        .setFeaturesCol("indexedFeatures")
        .setPredictionCol("predictedLabel")

    val (trainingDuration, oneRModel) = time(zeroR.fit(indexedDataset))

    val (predictionDuration, prediction) = time(oneRModel.transform(indexedDataset))

    val predictionsAndLabels = prediction.select("predictedLabel", "idx_Class")

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("idx_Class")
      .setPredictionCol("predictedLabel")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictionsAndLabels)

    println(s" Training Time ${trainingDuration} milliseconds\n")

    println(s" Prediction Time ${predictionDuration} milliseconds\n")

    println(s"The accuracy is : ${accuracy * 100} % ")

    spark.stop()

  }

  private def time[R](block: => R): (Long, R) = {
    val t0     = System.nanoTime()
    val result = block // call-by-name
    val t1     = System.nanoTime()
    (NANOSECONDS.toMillis(t1 - t0), result)
  }
}
