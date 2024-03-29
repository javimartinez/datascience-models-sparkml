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

import scala.annotation.tailrec

case class FuzzyPartition(regions: Vector[FuzzyRegion]) {

  def findFuzzyRegionWithMaxDegreeFor(item: Double): Int = // index of fuzzy region
    regions.zipWithIndex.map {
      case (fuzzyRegion: FuzzyRegion, index: Int) => (fuzzyRegion.membershipOf(item), index)
    }.maxBy(_._1)._2 // max of Degree
}

object FuzzyPartition {

  def apply(minValue: Double, //
            maxValue: Double,
            numFuzzyRegions: Int): FuzzyPartition = {

    val lastFuzzyRegion = numFuzzyRegions
    // TODO:
    val amplitude = (maxValue - minValue) / (numFuzzyRegions - 1) // because the last and the end partition has the amplitude/2 ???

    @tailrec
    def loop(left: Double,
             center: Double,
             right: Double,
             numFuzzyRegion: Int,
             regions: Vector[FuzzyRegion]): FuzzyPartition =
      numFuzzyRegion match {
        case 1 => new FuzzyPartition(regions.+:(LeftFuzzyRegion(center, right))) //Ending
        case fuzzyRegion if fuzzyRegion == lastFuzzyRegion => // beginning
          loop(left - amplitude,
               center - amplitude,
               right - amplitude,
               numFuzzyRegion - 1,
               Vector(RightFuzzyRegion(left, center)))
        case _ =>
          val rg = regions.+:(TriangularFuzzyRegion(left, center, right))
          loop(left - amplitude, center - amplitude, right - amplitude, numFuzzyRegion - 1, rg)
      }

    loop(maxValue - amplitude,
         maxValue,
         maxValue + amplitude,
         numFuzzyRegions,
         Vector[FuzzyRegion]())

  }

  def apply(n: Int): FuzzyPartition = {
    val fuzzyRegions = Range(0, n).map { element: Int =>
      FuzzyRegionSingleton(element)
    }.toVector

    FuzzyPartition(fuzzyRegions)
  }

}
