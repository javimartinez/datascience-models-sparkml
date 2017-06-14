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
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}


class ShuttleWMConfig(val pathDataFolder: String, val pathResultFolder: String, val algorithm: String, val numPartitions: Int) extends Config {


val inputsColumns   = Array("A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9")

  override val outputColumn     = "Class"
  override val outputColumnIdx = "idx_Class"
  override val dataSetName: String = "shuttle"


  val assembler =
    new VectorAssembler().setInputCols(inputsColumns).setOutputCol("features")

  val stringIndexerLabel =
    new StringIndexer().setInputCol(outputColumn).setOutputCol(outputColumnIdx)


  val pipelineToTransform =
    new Pipeline().setStages(Array(assembler, stringIndexerLabel))
}
