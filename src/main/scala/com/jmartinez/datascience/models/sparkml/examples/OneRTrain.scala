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

import com.jmartinez.datascience.models.sparkml.models.OneRClassifier

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.{CrossValidator, CrossValidatorModel, Pipeline}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


object OneRTrain {

  def oneRTrain(config: Config)(implicit spark: SparkSession): Unit = {

    println(s"OneR for dataset ${config.dataSetName} start")

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(config.outputColumnIdx)
      .setPredictionCol("prediction")
      .setMetricName("accuracy")


    val oneR: OneRClassifier =
      new OneRClassifier()
        .setLabelCol(config.outputColumnIdx)
        .setFeaturesCol("features")
        .setPredictionCol("prediction")
        .setX(1.0)

    // cross validation
    val paramGrid =
      new ParamGridBuilder().addGrid(oneR.reg, Array(1.0)).build()

    val cv =
      new CrossValidator()
        .setEstimator(new Pipeline().setStages(Array(oneR)))
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(paramGrid)
        .setTransformator(config.pipelineToTransform)
        .setNumFolds(5)
        .setNumPartitions(config.numPartitions)

    val dataFrameOfPaths = {
      val rdd = spark.sparkContext.parallelize(generatePaths(config.basePath, config.dataSetName, 5).map {
        case (trainFile, testFile) =>
          Row(trainFile, testFile)
      })
      spark.createDataFrame(rdd, generateSchema("trainPath testPath"))
    }

    val cvModel: CrossValidatorModel = cv.fit(dataFrameOfPaths)

    cvModel.metrictsAsString.foreach(println)
    spark.sparkContext.parallelize(cvModel.metrictsAsString, 1).saveAsTextFile(config.pathResult)

    println(s"OneR for dataset ${config.dataSetName} finish")
  }

  def generatePaths(basePath: String, dataSetName: String, nFolds: Int): Array[(String, String)] =
    (1 to nFolds).map { n =>
      val base = s"$basePath$dataSetName-$nFolds-$n"
      (s"${base}tra.dat", s"${base}tst.dat")
    }.toArray

  def generateSchema(schemaString: String): StructType = {
    // Generate the schema based on the string of schema
    val fields =
      schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    StructType(fields)
  }

}
