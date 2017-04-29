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

package com.jmartinez.datascience.models.sparkml.Fuzzy

import org.scalatest.{ FlatSpec, Matchers }

//  Input Variable 0: PART(TRAPLE(17.0,387.75) TRIAN(17.0,387.75,758.5) TRIAN(387.75,758.5,1129.25) TRIAN(758.5,1129.25,1500.0) TRAPRI(1129.25,1500.0) )

//Variable0tiene maximo: 1500.0 y minimo17.0
//Variable1tiene maximo: 32000.0 y minimo64.0
//Variable2tiene maximo: 64000.0 y minimo64.0
//Variable3tiene maximo: 256.0 y minimo0.0
//Variable4tiene maximo: 52.0 y minimo0.0
//Variable5tiene maximo: 176.0 y minimo0.0

class FuzzyPartitionSpec extends FlatSpec with Matchers {

  "createFuzzyPartition" should "generate adecuate partitions for input variable" in {

    //variable 0 machine CPU dataset
    val minimo: Double    = 17
    val maximo: Double    = 1500
    val numFuzzyPartition = 5

    val expectedFuzzyPartition =
      FuzzyPartition(
        Vector(LeftFuzzyRegion(17.0, 387.75),
               TriangularFuzzyRegion(17.0, 387.75, 758.5),
               TriangularFuzzyRegion(387.75, 758.5, 1129.25),
               TriangularFuzzyRegion(758.5, 1129.25, 1500.0),
               RightFuzzyRegion(1129.25, 1500.0)))

    FuzzyPartition(minimo, maximo, numFuzzyPartition) shouldBe expectedFuzzyPartition
  }

}
