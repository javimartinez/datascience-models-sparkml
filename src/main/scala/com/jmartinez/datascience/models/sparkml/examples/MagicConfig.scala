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


class MagicConfig(val pathDataFolder: String, val pathResultFolder: String, val algorithm: String, val numPartitions: Int) extends OneRConfig {

  override val dataSetName: String = "magic"

  override val outputColumn: String = "Class"
  override val outputColumnIdx: String = "Class_idx"

  val columnsToBucked = Array(
    "FLength",
    "FWidth",
    "FSize",
    "FConc",
    "FConc1",
    "FAsym",
    "FM3Long",
    "FM3Trans",
    "FAlpha",
    "FDist"
  )

  private val inputsColumns: Array[String] = columnsToBucked.map(str => s"${str}_b")

  val splits = Array(
    /*FLength*/ Array(4.2835, 59.26575, 114.24799999999999, 169.23024999999998, 224.21249999999998, 279.19475, 334.177),
    /*FWidth */ Array(0.0, 42.730333333333334, 85.46066666666667, 128.191, 170.92133333333334, 213.65166666666667, 256.382),
    /*FSize  */ Array(1.9413, 2.504966666666667, 3.0686333333333335, 3.6323000000000003, 4.195966666666667, 4.759633333333333, 5.3233),
    /*FConc  */ Array(0.0131, 0.15975, 0.3064, 0.45305, 0.5997, 0.7463500000000001, 0.893),
    /*FConc1 */ Array(3.0E-4, 0.11278333333333333, 0.22526666666666667, 0.33775, 0.4502333333333333, 0.5627166666666666, 0.6752),
    /*FAsym  */ Array(-457.9161, -285.7233, -113.53049999999999, 58.662300000000016, 230.85510000000002, 403.0479, Double.PositiveInfinity),
    /*FM3Long*/ Array(-331.78, -236.76316666666662, -141.74633333333327, -46.72949999999993, 48.28733333333341, 143.30416666666673, Double.PositiveInfinity),
    /*FM3Tran*/ Array(-205.8947, -141.60375, -77.3128, -13.02185, 51.269099999999995, 115.56004999999999, 179.851),
    /*FAlpha */ Array(0.0, 15.0, 30.0, 45.0, 60.0, 75.0, 90.0),
    /*FDist  */ Array(1.2826, 83.66233333333334, 166.04206666666667, 248.42180000000002, 330.80153333333334, 413.18126666666666, 495.561))


  private val bucketizers: Array[Bucketizer] = columnsToBucked.zipWithIndex.map {
    case (c, i) =>
      new Bucketizer()
        .setInputCol(c)
        .setOutputCol(s"${c}_b")
        .setSplits(splits(i))
  }

  private val stringIndexerLabel =
    new StringIndexer().setInputCol(outputColumn).setOutputCol(outputColumnIdx)

  private val assembler =
    new VectorAssembler().setInputCols(inputsColumns).setOutputCol("features")

  val pipelineToTransform =
    new Pipeline().setStages(bucketizers ++ Array(stringIndexerLabel, assembler))
}
