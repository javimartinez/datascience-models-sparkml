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

import com.jmartinez.datascience.models.sparkml.keelReader.KeelReader._
import com.jmartinez.datascience.models.sparkml.models.OneRClassifier
import org.apache.log4j.{ Level, Logger }

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{ StringIndexer, VectorAssembler, VectorIndexer }
import org.apache.spark.ml.tuning.{ CrossValidator, CrossValidatorModel, ParamGridBuilder }
import org.apache.spark.sql.SparkSession

object OneRClassifierExample {

  def main(args: Array[String]) {

    // Disable INFO Log
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val log = Logger.getRootLogger
    log.setLevel(Level.INFO)

    // Creates a DataFrame

    val spark =
      SparkSession.builder.appName("OneRClassifierExample").getOrCreate()

    // Crates a DataFrame
    val dataDF = spark.keelFile(args(0),10)

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

    val indexedDataset = prepareDataPipeline.fit(dataDF).transform(dataDF).cache()

    val onerR: OneRClassifier =
      new OneRClassifier()
        .setLabelCol("idx_Class")
        .setFeaturesCol("indexedFeatures")
        .setPredictionCol("predictedLabel")

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("idx_Class")
      .setPredictionCol("predictedLabel")
      .setMetricName("accuracy")

    val paramGrid = new ParamGridBuilder()
      .addGrid(onerR.reg, Array(1.0, 2.0)) // don't matter
      .build()

    val cv = new CrossValidator()
      .setEstimator(new Pipeline().setStages(Array(onerR)))
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)

    val model: CrossValidatorModel = cv.fit(indexedDataset)
    val result                     = model.transform(indexedDataset)

    val accuracy = evaluator.evaluate(result)

//    println(s" Training Time ${ trainingDuration } milliseconds\n")
//
//    println(s" Prediction Time ${ predictionDuration } milliseconds\n")
//
    log.info(s"The accuracy is : ${accuracy * 100} % ")

    log.info(s"Model: ${model.bestModel.toString()}")

    spark.stop()

  }

  private def time[R](block: => R): (Long, R) = {
    val t0     = System.nanoTime()
    val result = block // call-by-name
    val t1     = System.nanoTime()
    (NANOSECONDS.toMillis(t1 - t0), result)
  }

}
