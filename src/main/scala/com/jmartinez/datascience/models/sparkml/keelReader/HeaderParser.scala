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

package com.jmartinez.datascience.models.sparkml.keelReader

import fastparse.all._

sealed abstract class KeelAttribute(name: String)

// divide into Integer/RealAttribute
case class NumericAttribute(name: String, minValue: Double, maxValue: Double)
    extends KeelAttribute(name)

case class NominalAttribute(name: String) extends KeelAttribute(name)

object HeaderParser extends BasicsParser {

  val attributeName = P(CharsWhile(x => x != ' ').!)

  val attributeType: Parser[String] = P("integer".! | "real".!)

  val categoricalAttribute: Parser[KeelAttribute] =
    P("@attribute" ~ whiteSpaces ~ attributeName ~ whiteSpaces ~ bracedBlock(alphaNumeric) ~ End).map {
      case (atname, x) => // TODO: Esto no se puede quedar así, necesita refactor urgente. Solo para avanzar en el algoritmo
        NominalAttribute(atname) // TODO: en x tenemos los valores que toma el attributo
    }

  val numericAttribute: Parser[KeelAttribute] = P(
    "@attribute" ~ whiteSpaces ~ attributeName ~ whiteSpaces ~ attributeType ~ whiteSpaces ~ squareBrackedBlock(
      double) ~ End) // TODO: workaround
  .map {
    case (atname, atType, vector) =>
      atType match {
        case "integer" => NumericAttribute(atname, vector(0), vector(1))
        case "real"    => NumericAttribute(atname, vector(0), vector(1))
      }
  }

  val relationParser = P("@relation" ~ AnyChar.!)

  val inputsParser = P("@inputs")

  val outputParser = P("@outputs" ~ whiteSpaces ~ CharIn('a' to 'z').!)

  val keelHeaderParser: Parser[KeelAttribute] = P(categoricalAttribute | numericAttribute)

}
