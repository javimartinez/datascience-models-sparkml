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

import org.apache.spark.sql.SparkSession
import OneRTrain._

object OneRClassifierEx2 {

  def main(args: Array[String]): Unit = {

    // Disable INFO Log

    val logger = Logger.getLogger("my logger")

    logger.setLevel(Level.INFO)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    implicit val spark =
      SparkSession.builder.appName("WangMendelAlgorithm")
        .master("local[4]")
        .getOrCreate()

    //    oneRTrain(new Connect4Config(10))
    //    oneRTrain(new KDDCupConfig())
    //    oneRTrain(new Poker(10))
    oneRTrain(new ShuttleConfig(args(0), args(1), args(2), args(3).toInt))
//    oneRTrain(new MagicConfig(args(0), args(1), args(2), args(3).toInt))


  }

}