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
import fastparse.all.{ CharIn, CharsWhile }
import fastparse.noApi.{ P, _ }

trait BasicsParser extends Helpers {

  val White = WhitespaceApi.Wrapper {
    import fastparse.all._
    NoTrace(" ".rep)
  }

  import White._

  val whiteSpaces = P(" ".rep)

  // A parser for digits
  val digits = P(CharsWhile(Digits))

  // A parser for integral numbers
  val integral = P("0" | CharIn('1' to '9') ~ digits.?)

  // A parser for real numbers
  val double = // white space in the beging for situations like [0, 15]
  P(" ".? ~ "-".? ~ integral ~ ".".? ~ integral.rep(min = 1, max = 16).?).!.map(_.toDouble) //TODO: tener en cuenta negativos

  val alphaNumeric = P(CharIn('1' to '9') | CharIn('a' to 'z') | CharIn('A' to 'Z')).rep.!

  val openSquareBracket = P("[")

  val closeSquareBracked = P("]")

  val openBrace = P("{")

  val closeBrace = P("}")

  // A parser for a squareBracked block
  protected[this] def squareBrackedBlock[T](p: Parser[T]): Parser[Vector[T]] =
    openSquareBracket ~ p.rep(sep = ",").map(_.toVector) ~ closeSquareBracked

  // A parser for a brace block
  protected[this] def bracedBlock[T](p: Parser[T]): Parser[Vector[T]] =
    openBrace ~ p.rep(sep = ",").map(_.toVector) ~ closeBrace

}

object BasicsParser extends BasicsParser
