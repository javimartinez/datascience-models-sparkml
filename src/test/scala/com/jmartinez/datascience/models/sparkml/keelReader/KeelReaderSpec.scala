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

import org.scalatest._

import org.apache.spark.sql.types._

class KeelReaderSpec extends FlatSpec with Matchers with PrivateMethodTester {

  val parseHeaderAttributeLine = PrivateMethod[Option[StructField]]('parseKeelAttributeLine)

  "A parseHeaderAttributeLine " should "parse an integer Keel attribute" in {

    val attributeLine = "@attribute Y-box integer [0, 15]"

    KeelReader invokePrivate parseHeaderAttributeLine(attributeLine) shouldBe NumericAttribute(
      "Y-box",
      0,
      15
    )

  }

  it should "parse other integer keel attribute" in {

    val attributeLine = "@attribute S2 integer[1,4]"

    KeelReader invokePrivate parseHeaderAttributeLine(attributeLine) shouldBe NumericAttribute(
      "S2",
      1,
      4
    )

  }

  it should "parse a real keel attribute" in {

    val attributeLine = "@attribute AGE real [0.0,99.0]"

    KeelReader invokePrivate parseHeaderAttributeLine(attributeLine) shouldBe
      NumericAttribute("AGE", 0.0, 99.0)

  }

  it should "parse negative integer keel attribute" in {

    val attributeLine = "@attribute S2 integer[-1,-4]"

    KeelReader invokePrivate parseHeaderAttributeLine(attributeLine) shouldBe NumericAttribute(
      "S2",
      -1,
      -4
    )

  }

  it should "parse a nominal attribute" in {

    val attributeLine = "@attribute A11 {x,o,b}"

    KeelReader invokePrivate parseHeaderAttributeLine(attributeLine) shouldBe
      CategoricalAttribute("A11")
  }

}
