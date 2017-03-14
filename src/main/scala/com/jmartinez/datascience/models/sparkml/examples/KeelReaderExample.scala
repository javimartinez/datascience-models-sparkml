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

import org.apache.spark.sql.SparkSession
import com.jmartinez.datascience.models.sparkml.keelReader.KeelReader._
import org.apache.log4j.{ Level, Logger }

object KeelReaderExample {

  def main(args: Array[String]): Unit = {

    // Disable INFO Log
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark =
      SparkSession.builder.appName("KeelDataFrame").getOrCreate()

    // $example on$

    // Crates a DataFrame
    val dataframe =
      spark.keelFile("/Users/Javi/Development/datascience-models-sparkml/data/poker.dat")

    println(s"The size of DataFrame is: ${dataframe.count}")

    println(s"The schema is: ${dataframe.schema.toString}")

    dataframe.show(20)

  }

}
