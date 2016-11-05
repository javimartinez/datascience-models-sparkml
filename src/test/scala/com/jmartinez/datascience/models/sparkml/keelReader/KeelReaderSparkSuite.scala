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

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

class KeelReaderSparkSuite extends FunSuite with SharedSparkContext { // TODO: jmartinez esto hace ver que hay que separar la funcionalidad de esta clase

  test("test initializing spark context") {
    val list = List(1, 2, 3, 4)
    val rdd  = sc.parallelize(list)

    assert(rdd.count === 4)
  }

}
