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
import org.apache.log4j.{Level, Logger}

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.{CrossValidator, CrossValidatorModel, Pipeline}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object WangMendelAlgorithmExampleShuttle {

  def main(args: Array[String]) {

    // Disable INFO Log

    val logger = Logger.getLogger("my logger")

    logger.setLevel(Level.INFO)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark =
      SparkSession.builder.appName("WangMendelAlgorithm")
        .master("local[4]")
        .getOrCreate()

    logger.info("Wang&Mendel pipeline starts")

    //Shuffle

    //    val inputsColumns   = Array("A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9")
    //    val ouputColumn     = "Class"
    //    val outputColumnIdx = "idx_Class"
    //    val basePath        = args(0)
    //    val dataSetName     = args(1)
    //    val basePath = "/Users/Javi/development/data/shuttle-5-fold/"
    //    val dataSetName = "shuttle"

    //Poker

    //    val basePath = args(0)
    //    val dataSetName = args(1)
    //    val basePath        = "/Users/Javi/development/data/poker-5-fold/"
    //    val dataSetName     = "poker"

    // fars

    //    val columsString = Array(
    //      "CASE_STATE",
    //      "SEX",
    //      "PERSON_TYPE",
    //      "SEATING_POSITION",
    //      "RESTRAINT_SYSTEM-USE",
    //      "AIR_BAG_AVAILABILITY/DEPLOYMENT",
    //      "EJECTION",
    //      "EJECTION_PATH",
    //      "EXTRICATION",
    //      "NON_MOTORIST_LOCATION",
    //      "POLICE_REPORTED_ALCOHOL_INVOLVEMENT",
    //      "METHOD_ALCOHOL_DETERMINATION",
    //      "ALCOHOL_TEST_TYPE",
    //      "ALCOHOL_TEST_RESULT",
    //      "POLICE-REPORTED_DRUG_INVOLVEMENT",
    //      "METHOD_OF_DRUG_DETERMINATION",
    //      "DRUG_TEST_TYPE_(1_of_3)",
    //      "DRUG_TEST_TYPE_(2_of_3)",
    //      "DRUG_TEST_TYPE_(3_of_3)",
    //      "HISPANIC_ORIGIN",
    //      "TAKEN_TO_HOSPITAL",
    //      "RELATED_FACTOR_(1)-PERSON_LEVEL",
    //      "RELATED_FACTOR_(2)-PERSON_LEVEL",
    //      "RELATED_FACTOR_(3)-PERSON_LEVEL",
    //      "RACE")
    //
    //    val columsRealIdx = Array("AGE", "DRUG_TEST_RESULTS_(1_of_3)", "DRUG_TEST_RESULTS_(2_of_3)","DRUG_TEST_RESULTS_(3_of_3)")
    //    val columsStringIdx = columsString.map(att => s"${att}_idx")
    //    val inputsColumns = columsStringIdx ++ columsRealIdx
    //    val ouputColumn = "INJURY_SEVERITY"
    //    val outputColumnIdx = "INJURY_SEVERITY_idx"

    //magic
    val inputsColumns = Array("FLength",
      "FWidth",
      "FSize",
      "FConc",
      "FConc1",
      "FAsym",
      "FM3Long",
      "FM3Trans",
      "FAlpha",
      "FDist")

    val ouputColumn = "Class"
    val outputColumnIdx = "Class_idx"
    val basePath = args(0)
    val dataSetName = args(1)


    val assembler =
      new VectorAssembler().setInputCols(inputsColumns).setOutputCol("features")

    val stringIndexerLabel =
      new StringIndexer().setInputCol(ouputColumn).setOutputCol(outputColumnIdx)


    //    val stringIndexersFeatures: Array[StringIndexer] = columsString.zipWithIndex.map {
    //      case (feature,index)  =>
    //        new StringIndexer().setInputCol(feature).setOutputCol(columsStringIdx(index))
    //    }

    val pipelineToTransform =
      new Pipeline().setStages(Array(assembler, stringIndexerLabel))


    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(outputColumnIdx)
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val wangMendelAlgorithm =
      new WangMendelAlgorithm()
        .setLabelCol(outputColumnIdx)
        .setPredictionCol("prediction")
        .setNumFuzzyRegions(args(2).toInt)

    // cross validation
    val paramGrid =
      new ParamGridBuilder().addGrid(wangMendelAlgorithm.numFuzzyRegions, Array(args(2).toInt)).build()

    val cv =
      new CrossValidator()
        .setEstimator(new Pipeline().setStages(Array(wangMendelAlgorithm)))
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(paramGrid)
        .setTransformator(pipelineToTransform)
        .setNumFolds(5)
        .setNumPartitions(500)

    val dataFrameOfPaths = {
      val rdd = spark.sparkContext.parallelize(generatePaths(basePath, dataSetName, 5).map {
        case (trainFile, testFile) =>
          Row(trainFile, testFile)
      })
      spark.createDataFrame(rdd, generateSchema("trainPath testPath"))
    }

    val cvModel: CrossValidatorModel = cv.fit(dataFrameOfPaths)

    cvModel.metrictsAsString.foreach(println)
    spark.sparkContext.parallelize(cvModel.metrictsAsString, 1).saveAsTextFile(args(3))

    println("Spark job finished")

    spark.stop()

  }

}
