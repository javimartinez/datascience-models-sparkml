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
import org.apache.spark.ml.feature.{Bucketizer, StringIndexer, VectorAssembler}

class KDDCupConfig(val pathDataFolder: String, val pathResultFolder: String, val algorithm: String, val numPartitions: Int) extends OneRConfig {


  override val outputColumn: String = "Class"
  override val outputColumnIdx = "idx_Class"
  override val dataSetName: String = "kddcup"


  //KDDCup
  private val columnsToBucked = Array(
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
  private val columnsToStringIndex = allColumns.filter(str => !columnsToBucked.contains(str))

  private val inputsColumns: Array[String] = allColumns.map(str => s"${str}_b").toArray

  private val basicSplit = Array(Double.NegativeInfinity, 0.0, 1.0, Double.PositiveInfinity)

  private val splits: Array[Array[Double]] =
    Array(
      /*Atr-0*/ Array(0.0, 9721.5, 19443.0, 29164.5, 38886.0, 48607.5, Double.PositiveInfinity),
      /*Atr-4 */ Array(0.0, 1.1556260666666667E8, 2.3112521333333334E8, 3.4668782E8, 4.622504266666667E8, 5.778130333333334E8, Double.PositiveInfinity),
      /*Atr-5 */ Array(0.0, 859244.6666666666, 1718489.3333333333, 2577734.0, 3436978.6666666665, 4296223.333333333, 5155468.0),
      /*Atr-9 */ Array(0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0),
      /*Atr-12*/ Array(0.0, 147.33333333333334, 294.6666666666667, 442.0, 589.3333333333334, 736.6666666666667, Double.PositiveInfinity),
      /*Atr-15*/ Array(0.0, 165.5, 331.0, 496.5, 662.0, 827.5, 993.0),
      /*Atr-16*/ Array(0.0, 4.666666666666667, 9.333333333333334, 14.0, 18.666666666666668, 23.333333333333336, Double.PositiveInfinity),
      /*Atr-22*/ Array(0.0, 85.16666666666667, 170.33333333333334, 255.5, 340.6666666666667, 425.83333333333337, Double.PositiveInfinity),
      /*Atr-23*/ Array(0.0, 85.16666666666667, 170.33333333333334, 255.5, 340.6666666666667, 425.83333333333337, Double.PositiveInfinity),
      /*Atr-24*/ basicSplit,
      /*Atr-25*/ basicSplit,
      /*Atr-26*/ basicSplit,
      /*Atr-27*/ basicSplit,
      /*Atr-28*/ basicSplit,
      /*Atr-29*/ basicSplit,
      /*Atr-30*/ basicSplit,
      /*Atr-31*/ Array(0.0, 42.5, 85.0, 127.5, 170.0, 212.5, Double.PositiveInfinity),
      /*Atr-32*/ Array(0.0, 42.5, 85.0, 127.5, 170.0, 212.5, Double.PositiveInfinity),
      /*Atr-33*/ basicSplit,
      /*Atr-34*/ basicSplit,
      /*Atr-35*/ basicSplit,
      /*Atr-36*/ basicSplit,
      /*Atr-37*/ basicSplit,
      /*Atr-38*/ basicSplit,
      /*Atr-39*/ basicSplit,
      /*Atr-40*/ basicSplit
    )

  println("columnsToBucked" + columnsToBucked.length)
  println("splits" + splits.length)

  private val bucketizers: Array[Bucketizer] = columnsToBucked.zipWithIndex.map {
    case (c, i) =>
      new Bucketizer()
        .setInputCol(c)
        .setOutputCol(s"${c}_b")
        .setSplits(splits(i))
  }

  private val stringIndexerLabel =
    new StringIndexer().setInputCol(outputColumn).setOutputCol(outputColumnIdx)

  private val stringIndexerFeatures: Array[StringIndexer] = columnsToStringIndex.map { str =>
    new StringIndexer().setInputCol(str).setOutputCol(s"${str}_b")
  }.toArray

  private val assembler =
    new VectorAssembler().setInputCols(inputsColumns).setOutputCol("features")

  val pipelineToTransform =
    new Pipeline().setStages(bucketizers ++ stringIndexerFeatures ++ Array(stringIndexerLabel, assembler))

}
