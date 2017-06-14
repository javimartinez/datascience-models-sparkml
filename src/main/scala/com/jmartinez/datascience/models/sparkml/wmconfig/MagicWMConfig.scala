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


class MagicWMConfig(val pathDataFolder: String, val pathResultFolder: String, val algorithm: String, val numPartitions: Int) extends Config {

  override val outputColumn = "Class"
  override val outputColumnIdx = "Class_idx"
  override val dataSetName: String = "magic"

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



  val assembler =
    new VectorAssembler().setInputCols(inputsColumns).setOutputCol("features")

  val stringIndexerLabel =
    new StringIndexer().setInputCol(outputColumn).setOutputCol(outputColumnIdx)


  val pipelineToTransform =
    new Pipeline().setStages(Array(assembler, stringIndexerLabel))
}
