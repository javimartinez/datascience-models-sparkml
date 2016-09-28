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

package com.jmartinez.datascience.models.sparkml.models

import org.apache.spark.annotation._
import org.apache.spark.ml.attribute.{ Attribute, AttributeType }
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.ml.{ Estimator, Model, ZeroRParams }
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off

final class ZeroRClassifierModel(override val uid: String, val predictedLabel: Double)
    extends Model[ZeroRClassifierModel]
    with ZeroRParams {

  override def copy(extra: ParamMap): ZeroRClassifierModel = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = true, new VectorUDT)

  }

  /**
    * Transforms the input dataset.
    */
  override def transform(dataset: Dataset[_]): DataFrame = {

    // It's necessary check the shema with transformSchema

    val addPredictedLabel = udf { () =>
      predictedLabel
    }

    dataset.withColumn($(predictionCol), addPredictedLabel())

  }

}

final class ZeroRClassifier(override val uid: String) extends Estimator[ZeroRClassifierModel] with ZeroRParams {

  def this() = this(Identifiable.randomUID("zeroR"))

  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  // @Since("1.5.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  // @Since("1.5.0")

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  override def fit(dataset: Dataset[_]): ZeroRClassifierModel = {

    val newDataset = dataset.select($(labelCol))

    val predictedClass =
      Attribute.fromStructField(dataset.schema($(labelCol))).attrType match {

        case AttributeType.Nominal => {
          val Row(maxLabel: Double) =
            newDataset.groupBy($(labelCol)).count().agg(max($(labelCol))).head()
          maxLabel
        }

        case AttributeType.Numeric => {
          val Row(maxLabel: Double) =
            newDataset.agg(avg($(labelCol))).head()
          maxLabel
        }
        case _ =>
          throw new UnsupportedOperationException(
            "The label Column need be AttributeType.Nominal Or AttributeType.Numeric"
          )
      }

    val model = new ZeroRClassifierModel(uid, predictedClass).setParent(this)
    copyValues(model)
  }

  override def copy(extra: ParamMap): Estimator[ZeroRClassifierModel] =
    defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = true, new VectorUDT)
  }

}
