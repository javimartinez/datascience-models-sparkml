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

package org.apache.spark.ml

import org.apache.spark.ml.param.{ DoubleParam, IntParam, ParamValidators }

trait ZeroRParams extends PredictorParams

trait WangMendelParams extends PredictorParams {

  final val numFuzzyRegions: IntParam =
    new IntParam(this, "numFuzzyRegions", "> 0 ", ParamValidators.gt(0))
}

trait OneRParams extends PredictorParams {

  /**
    * Param for regularization parameter (>= 0).
    * @group param
    */
  final val reg: DoubleParam =
    new DoubleParam(this, "regParam", "regularization parameter (>= 0)", ParamValidators.gtEq(0))

  /** @group getParam */
  final def getRegParam: Double = $(reg)
}
