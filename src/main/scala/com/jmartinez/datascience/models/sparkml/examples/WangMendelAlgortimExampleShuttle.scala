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

import com.jmartinez.datascience.models.sparkml.models.WangMendelAlgorithm
import org.apache.log4j.{ Level, Logger }

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{ StringIndexer, VectorAssembler }
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.{ CrossValidator, Pipeline }
import org.apache.spark.sql.types.{ StringType, StructField, StructType }
import org.apache.spark.sql.{ Row, SparkSession }

object WangMendelAlgortimExampleShuttle {

  def main(args: Array[String]) {

    // Disable INFO Log

    val logger = Logger.getLogger("my logger")

    logger.setLevel(Level.INFO)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark =
      SparkSession.builder
        .appName("WangMendelAlgorithm")
        .master("local[4]")
        .getOrCreate()

    logger.info("Wang&Mendel pipeline starts")

//    val inputsColumns = Array("A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9")
//    val ouputColumn = "Class"
//    val outputColumnIdx = "idx_Class"
//    val basePath = "/Users/Javi/development/datascience-models-sparkml/data/shuttle-5-fold/"
//    val dataSetName = "shuttle"

    val inputsColumns   = Array("S1", "C1", "S2", "C2", "S3", "C3", "S4", "C4", "S5", "C5")
    val ouputColumn     = "Class"
    val outputColumnIdx = "idx_Class"
    val basePath        = "/Users/Javi/development/data/poker-5-fold/"
    val dataSetName     = "poker"


    val assembler =
      new VectorAssembler().setInputCols(inputsColumns).setOutputCol("features")

    val stringIndexer = new StringIndexer().setInputCol(ouputColumn).setOutputCol(outputColumnIdx)

    val pipelineToTransform = new Pipeline().setStages(Array(assembler, stringIndexer))

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(outputColumnIdx)
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val wangMendelAlgorithm =
      new WangMendelAlgorithm()
        .setLabelCol(outputColumnIdx)
        .setPredictionCol("prediction")
        .setNumFuzzyRegions(3)

    // cross validation
    val paramGrid = new ParamGridBuilder()
      .addGrid(wangMendelAlgorithm.numFuzzyRegions, Array(3))
      .build()

    val cv =
      new CrossValidator()
        .setEstimator(new Pipeline().setStages(Array(wangMendelAlgorithm)))
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(paramGrid)
        .setTransformator(pipelineToTransform)
        .setNumFolds(5)

    val dataFrameOfPaths = {
      val rdd = spark.sparkContext.parallelize(generatePaths(basePath, dataSetName, 5).map {
        case (trainFile, testFile) =>
          Row(trainFile, testFile)
      })
      spark.createDataFrame(rdd, generateSchema("trainPath testPath"))
    }

    cv.fit(dataFrameOfPaths)

    println("Spark job finished")

    spark.stop()
  }

  def generatePaths(basePath: String, dataSetName: String, nFolds: Int): Array[(String, String)] =
    (1 to nFolds).map { n =>
      val base = s"$basePath$dataSetName-$nFolds-$n"
      (s"${base}tra.dat", s"${base}tst.dat")
    }.toArray

  def generateSchema(schemaString: String): StructType = {
    // Generate the schema based on the string of schema
    val fields = schemaString
      .split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    StructType(fields)
  }
}
