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

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}


class ShuttleConfig(val pathDataFolder: String, val pathResultFolder: String, val algorithm: String, val numPartitions: Int) extends Config {


  val columnsToStringIndex = (1 to 9).map(x => s"A${x}").toArray
  val inputColumns = columnsToStringIndex.map(str => s"${str}_b")

  override val outputColumn: String = "Class"
  override val outputColumnIdx: String = "Class_idx"

  override val dataSetName: String = "shuttle"


  private val stringIndexerLabel =
    new StringIndexer().setInputCol(outputColumn).setOutputCol(outputColumnIdx)

  private val stringIndexerFeatures: Array[StringIndexer] = columnsToStringIndex.map { str =>
    new StringIndexer().setInputCol(str).setOutputCol(s"${str}_b")
  }

  private val assembler =
    new VectorAssembler().setInputCols(inputColumns).setOutputCol("features")


  override val pipelineToTransform: Pipeline =
    new Pipeline().setStages(stringIndexerFeatures ++ Array(stringIndexerLabel, assembler))

}
