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

import fastparse.WhitespaceApi
import fastparse.noApi._

object HeaderParser {

  case class KeelAttribute(name: String, attributeType: String)

  val White = WhitespaceApi.Wrapper {
    import fastparse.all._
    NoTrace(" ".rep)
  }

  import White._

  val whiteSpaces = P(" ".rep)

  val attributeName = P(CharsWhile(x => x != ' ').!)

  val attributeType: Parser[String] = P("integer".! | "real".! | "".!)

  val attributeParser: Parser[KeelAttribute] =
    P("@attribute" ~ whiteSpaces ~ attributeName ~ whiteSpaces ~ attributeType)
      .map(KeelAttribute.tupled)

  val relationParser = P("@relation" ~ AnyChar.!)

  val inputsParser = P("@inputs")

  val outputParser = P("@outputs" ~ whiteSpaces ~ CharIn('a' to 'z').!)

  val keelHeaderParser: Parser[KeelAttribute] = P(attributeParser)

}
