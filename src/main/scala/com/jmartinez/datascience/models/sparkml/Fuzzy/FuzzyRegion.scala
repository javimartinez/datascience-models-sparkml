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

sealed abstract class FuzzyRegion extends Serializable {

  def membershipOf(x: Double): Double

}

class LeftFuzzyRegion(val center: Double, val right: Double) extends FuzzyRegion {

  //TODO: rename x
  override def membershipOf(x: Double): Double = {
    val degreeOfMembership = x match {
      case _: Double if x < center => 1
      case _: Double if x < right  => (right - x) / (right - center)
      case _: Double               => 0
    }
    degreeOfMembership
  }
}

object LeftFuzzyRegion {

  def apply(center: Double, right: Double): LeftFuzzyRegion =
    new LeftFuzzyRegion(center, right)

}

class RightFuzzyRegion(val left: Double, val center: Double) extends FuzzyRegion {

  override def membershipOf(x: Double): Double = {
    val degreeOfMembership = x match {
      case _: Double if x < left   => 0
      case _: Double if x < center => 1 - (center - x) / (center - left)
      case _: Double               => 1
    }
    degreeOfMembership
  }
}

object RightFuzzyRegion {

  def apply(left: Double, centre: Double): RightFuzzyRegion =
    new RightFuzzyRegion(left, centre)
}

class TriangularFuzzyRegion(val left: Double, val center: Double, val right: Double)
    extends FuzzyRegion {

  override def membershipOf(x: Double): Double = {
    val degreeOfMembership = x match {
      case _: Double if x < left   => 0
      case _: Double if x < center => 1 - (center - x) / (center - left)
      case _: Double if x < right  => (right - x) / (right - center)
      case _: Double               => 0
    }
    degreeOfMembership
  }
}

object TriangularFuzzyRegion {

  def apply(left: Double, center: Double, right: Double) =
    new TriangularFuzzyRegion(left, center, right)

}
