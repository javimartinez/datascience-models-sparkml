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


import org.scalatest.Matchers
import org.scalatest.matchers.{ MatchResult, Matcher }
import fastparse.all._
import fastparse.core.Parsed.Success

trait ParserMatchers extends Matchers {

  type Failure = fastparse.core.Parsed.Failure[_, _]

  def parsedTo[T](result: T): Matcher[Parsed[T]] = new Matcher[Parsed[T]] {
    override def apply(left: Parsed[T]): MatchResult = {
      val (msg, passes) = left match {
        case Success(v, _)  => (v, result == v)
        case error: Failure => (s"with failure: ${error.msg}", false)
      }
      MatchResult(
        passes,
        s"The ${result.toString} was not equals to ${msg.toString}",
        s"The ${result.toString} was equals to ${msg.toString}"
      )
    }
  }

  def notParse[T] = new Matcher[Parsed[T]] {
    override def apply(left: Parsed[T]): MatchResult = {

      val (msg, passes) = left match {
        case Success(v, _)  => (v, false)
        case error: Failure => (error.msg, true)
      }
      MatchResult(
        passes,
        s"The parser `$left` should not parse, however it did parse to: $msg",
        s"Didn't parse with failure:$msg"
      )
    }
  }
}
object ParserMatchers extends ParserMatchers