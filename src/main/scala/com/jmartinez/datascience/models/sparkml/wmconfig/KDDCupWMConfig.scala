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


class KDDCupWMConfig(val pathDataFolder: String, val pathResultFolder: String, val algorithm: String, val numPartitions: Int) extends Config {


  override val outputColumn: String = "Class"
  override val outputColumnIdx = "idx_Class"
  override val dataSetName: String = "kddcup"

  private val columnsReal = Array(
    "Atr-0",
    "Atr-4",
    "Atr-5",
    "Atr-9",
    "Atr-12",
    "Atr-15",
    "Atr-16",
    "Atr-22",
    "Atr-23",
    "Atr-24",
    "Atr-25",
    "Atr-26",
    "Atr-27",
    "Atr-28",
    "Atr-29",
    "Atr-30",
    "Atr-33",
    "Atr-34",
    "Atr-35",
    "Atr-36",
    "Atr-37",
    "Atr-38",
    "Atr-39",
    "Atr-40",
    "Atr-31",
    "Atr-32"
  )

  private val allColumns = (0 to 40).map(x => s"Atr-${x}")
  private val columnsToStringIndex = allColumns.filter(str => !columnsReal.contains(str))

  private val inputsColumns: Array[String] = allColumns.map { str =>
    if (columnsReal.contains(str))
      str
    else
      s"${str}_b"
  }.toArray

  private val stringIndexerLabel =
    new StringIndexer().setInputCol(outputColumn).setOutputCol(outputColumnIdx)

  private val stringIndexerFeatures: Array[StringIndexer] = columnsToStringIndex.map { str =>
    new StringIndexer().setInputCol(str).setOutputCol(s"${str}_b")
  }.toArray

  private val assembler =
    new VectorAssembler().setInputCols(inputsColumns).setOutputCol("features")

  val pipelineToTransform =
    new Pipeline().setStages(stringIndexerFeatures ++ Array(stringIndexerLabel, assembler))

}
