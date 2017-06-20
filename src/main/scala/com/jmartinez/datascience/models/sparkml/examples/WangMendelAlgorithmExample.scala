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

import org.apache.log4j.{Level, Logger}

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.{CrossValidator, CrossValidatorModel, Pipeline}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import com.jmartinez.datascience.models.sparkml.models.WangMendelAlgorithm
import com.jmartinez.datascience.models.sparkml.wmconfig._

object WangMendelAlgorithmExample {

  def main(args: Array[String]) {

    // Disable INFO Log

    val logger = Logger.getLogger("my logger")

    logger.setLevel(Level.INFO)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    implicit val spark =
      SparkSession.builder
        .appName("WangMendelAlgorithm")
//        .master(args(0))
        .getOrCreate()

    logger.info("Wang&Mendel pipeline starts")


    val resultMagic: List[String] = "Resultados Magic \\n" +: WMTrain(new MagicWMConfig(args(1), args(2), args(3), args(4).toInt))
    println("Resultados Magic")
    resultMagic.foreach(println)

    val resultShuttle: List[String] = "Resultados Shutlle \\n" +: WMTrain(new ShuttleWMConfig(args(1), args(2), args(3), args(4).toInt))
    println("Resultados Shutlle")
    resultShuttle.foreach(println)


    val resultPoker: List[String] = "Resultados Poker \\n" +: WMTrain(new PokerWMConfig(args(1), args(2), args(3), args(4).toInt))
    println("Resultados Poker")
    resultPoker.foreach(println)


    val resultConnect4: List[String] = "Resultados Connect-4 \\n" +: WMTrain(new Connect4WMConfig(args(1), args(2), args(3), args(4).toInt))
    println("Resultados connect 4")
    resultConnect4.foreach(println)

    val resultKddcup: List[String] = "Resultados KDDcup \\n" +: WMTrain(new KDDCupWMConfig(args(1), args(2), args(3), args(4).toInt))
    println("Resultados KDDcup")

    spark.sparkContext.parallelize(resultPoker ++ resultMagic ++ resultShuttle ++ resultConnect4 ++ resultKddcup, 1)
      .saveAsTextFile(args(2))

    println("Spark job finished")

  }

  def WMTrain(config: Config)(implicit spark: SparkSession): List[String] = {


    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(config.outputColumnIdx)
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val wangMendelAlgorithm =
      new WangMendelAlgorithm()
        .setLabelCol(config.outputColumnIdx)
        .setPredictionCol("prediction")
        .setNumFuzzyRegions(5)

    // cross validation
    val paramGrid =
      new ParamGridBuilder().addGrid(wangMendelAlgorithm.numFuzzyRegions, Array(5)).build()

    val cv =
      new CrossValidator()
        .setEstimator(new Pipeline().setStages(Array(wangMendelAlgorithm)))
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

    cvModel.metrictsAsString
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
