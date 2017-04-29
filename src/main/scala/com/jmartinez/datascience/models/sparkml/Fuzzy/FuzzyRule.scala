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

/**
  *
  * @param antecedents
  * @param consequent
  * @param degree
  */
case class FuzzyRule(antecedents: Array[Int], consequent: Int, degree: Double) {

  def getLabelsOfAntecedentsCodified: String =
    // String or Int ??? => Better Int right?
    antecedents.foldLeft("")((acc, element) => s"$acc$element")

  override def toString: String = {
    val antecedentAsString = antecedents.zipWithIndex.map {
      case (regionIndex, index) =>
        s"feature $index is in Region $regionIndex \n" // TODO: Better with names !!!!
    }.foldLeft("")(_ + _)

    s"IF $antecedentAsString THEN $consequent"
  }
}
