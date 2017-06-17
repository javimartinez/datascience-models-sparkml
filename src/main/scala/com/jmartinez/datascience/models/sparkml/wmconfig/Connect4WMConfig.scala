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

package com.jmartinez.datascience.models.sparkml.wmconfig

import com.jmartinez.datascience.models.sparkml.examples.Config

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{ StringIndexer, StringIndexerCustom, VectorAssembler }

class Connect4WMConfig(val pathDataFolder: String,
                       val pathResultFolder: String,
                       val algorithm: String,
                       val numPartitions: Int)
    extends Config {

  override val dataSetName = "connect-4"

  private val columnsToStringIndex = (1 to 42).map(x => s"A${x}")

  private val inputsColumns: Array[String] = columnsToStringIndex.map(str => s"${str}_b").toArray

  inputsColumns.foreach(println)

  override val outputColumn    = "Class"
  override val outputColumnIdx = "idx_Class"

  private val stringIndexerLabel =
    new StringIndexer().setInputCol(outputColumn).setOutputCol(outputColumnIdx)

  private val stringIndexerFeatures: Array[StringIndexerCustom] = columnsToStringIndex.map { str =>
    new StringIndexerCustom().setInputCol(str).setOutputCol(s"${str}_b")
  }.toArray

  private val assembler =
    new VectorAssembler().setInputCols(inputsColumns).setOutputCol("features")

  override val pipelineToTransform: Pipeline =
    new Pipeline().setStages(stringIndexerFeatures ++ Array(stringIndexerLabel, assembler))

}
