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

  val parseKeelAttributeLine = PrivateMethod[KeelAttribute]('parseKeelAttributeLine)

  "A parseHeaderAttributeLine " should "parse an integer Keel attribute" in {

    val attributeLine = "@attribute Y-box integer [0, 15]"

    KeelReader invokePrivate parseKeelAttributeLine(attributeLine) shouldBe NumericAttribute(
      "Y-box",
      0,
      15
    )

  }

  it should "parse other integer keel attribute" in {

    val attributeLine = "@attribute S2 integer[1,4]"

    KeelReader invokePrivate parseKeelAttributeLine(attributeLine) shouldBe NumericAttribute(
      "S2",
      1,
      4
    )

  }

  it should "parse a real keel attribute" in {

    val attributeLine = "@attribute AGE real [0.0,99.0]"

    KeelReader invokePrivate parseKeelAttributeLine(attributeLine) shouldBe
      NumericAttribute("AGE", 0.0, 99.0)

  }

  it should "parse negative integer keel attribute" in {

    val attributeLine = "@attribute S2 integer[-1,-4]"

    KeelReader invokePrivate parseKeelAttributeLine(attributeLine) shouldBe NumericAttribute(
      "S2",
      -1,
      -4
    )

  }

  it should "parse a nominal attribute" in {

    val attributeLine = "@attribute A11 {x,o,b}"

    KeelReader invokePrivate parseKeelAttributeLine(attributeLine) shouldBe
      NominalAttribute("A11")
  }

  it should "parse a real attribute from magic dataset" in {

    val attributeLine = "@attribute FConc1 real [0.0030, 0.6752]"

    KeelReader invokePrivate parseKeelAttributeLine(attributeLine) shouldBe
    NumericAttribute("FConc1",0.0030,0.6752)
  }

  it should "parse an attribute " in {

    val attributeLine = "@attribute CASE_STATE {District_of_Columbia,0,1,2,3,4,5,6,7,8,9}"
    KeelReader invokePrivate parseKeelAttributeLine(attributeLine) shouldBe
      NominalAttribute("CASE_STATE")
  }

  it should "parse attribute from far dataset "  in {

    val attributeLine = "@attribute RESTRAINT_SYSTEM-USE {None_Used/Not_Applicable, Shoulder_Belt, Lap_Belt, Lap_and_Shoulder_Belt, Child_Safety_Seat, Motorcycle_Helmet, Bicycle_Helmet, Restraint_Used_-_Type_Unknown, Safety_Belt_Used_Improperly, Child_Safety_Seat_Used_Improperly, Helmets_Used_Improperly, Unknown}"
//        val attributeLine = "@attribute RESTRAINT_SYSTEM-USE {Alabama,Alaska,Arizona,Arkansas,California,Colorado,Connecticut,Delaware,District_of_Columbia,Florida,Georgia,Hawaii,Idaho,Illinois,Indiana,Iowa,Kansas,Kentucky,Louisiana,Maine,Maryland,Massachusetts,Michigan,Minnesota,Mississippi,Missouri,Montana,Nebraska,Nevada,New_Hampshire,New_Jersey,New_Mexico,New_York,North_Carolina,North_Dakota,Ohio,Oklahoma,Oregon,Pennsylvania,Puerto_Rico,Rhode_Island,South_Carolina,South_Dakota,Tennessee,Texas,Utah,Vermont,Virginia,Washington,West_Virginia,Wisconsin,Wyoming}"

    val att: KeelAttribute = KeelReader invokePrivate parseKeelAttributeLine(attributeLine)
//    println(att.toString)

    att shouldBe NominalAttribute("RESTRAINT_SYSTEM-USE")
  }

  it should "parse other attribute from far dataset "  in {

    val attributeLine = "@attribute INJURY_SEVERITY {No_Injury, Possible_Injury, Nonincapaciting_Evident_Injury, Incapaciting_Injury, Fatal_Injury, Injured_Severity_Unknown, Died_Prior_to_Accident, Unknown}"


//    val attributeLine = "@attribute RELATED_FACTOR_(1)-PERSON_LEVEL {hola,adios}"
    val att: KeelAttribute = KeelReader invokePrivate parseKeelAttributeLine(attributeLine)

    att shouldBe NominalAttribute("INJURY_SEVERITY")
  }
}
