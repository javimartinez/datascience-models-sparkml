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

trait Config {
  val outputColumn: String
  val outputColumnIdx: String
  val pathDataFolder: String
  val pathResultFolder: String
  val pipelineToTransform: Pipeline
  val algorithm: String
  val dataSetName: String
  val numPartitions: Int

  def pathResult: String = s"$pathResultFolder/$dataSetName-result-$algorithm"

  def basePath: String = s"$pathDataFolder/$dataSetName-5-fold/"

}