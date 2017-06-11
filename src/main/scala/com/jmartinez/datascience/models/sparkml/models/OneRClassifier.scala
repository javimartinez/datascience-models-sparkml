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

import org.apache.spark.annotation._
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.ml.{ Estimator, Model, OneRParams }
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ DoubleType, StructField, StructType }
import org.apache.spark.sql.{ DataFrame, _ }

case class OneRule(
    attribute: Attribute,
    labelAttribute: Attribute,
    predictedLabelAttribute: Map[Double, Double],
    totalScore: Double
) extends Serializable {

  override def toString: String = {

    val attributeValues = attribute.attrType match {
      case AttributeType.Nominal =>
        attribute.asInstanceOf[NominalAttribute].values.get
      case AttributeType.Binary =>
        attribute.asInstanceOf[BinaryAttribute].values.get
      case _ =>
        throw new Exception("The attribute type is not correct ")
    }

    val attributeLabelValues = labelAttribute.attrType match {
      case AttributeType.Nominal =>
        labelAttribute.asInstanceOf[NominalAttribute].values.get
      case AttributeType.Binary =>
        labelAttribute.asInstanceOf[BinaryAttribute].values.get
      case _ =>
        throw new Exception("The attribute type is not correct ")
    }

    predictedLabelAttribute.map {
      case (aValueIndex: Double, lValueIndex: Double) =>
        val attributeValue = attributeValues(aValueIndex.toInt)
        val labelValue     = attributeLabelValues(lValueIndex.toInt)

        s"If ${attribute.name.getOrElse("x")} = $attributeValue THEN ${labelAttribute.name
          .getOrElse("x")} = $labelValue \n"

    }.foldLeft("")(_ + _)

  }

}

final class OneRClassifierModel(override val uid: String, val rule: OneRule)
    extends Model[OneRClassifierModel]
    with OneRParams {

  override def copy(extra: ParamMap): OneRClassifierModel = {
    val copied = new OneRClassifierModel(uid, rule)
    copyValues(copied, extra).setParent(parent)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    // TODO: check feature column
    require(!schema.fieldNames.contains(predictionCol), s"Prediction column already exists")
    StructType(schema.fields :+ StructField("prediction", DoubleType, false))
  }

  /**
    * Transforms the input dataset.
    */
  override def transform(dataset: Dataset[_]): DataFrame = {

    val classifyInstance = udf { (instanceFeatures: Vector) =>
      val attributeValueToClassify: Double = instanceFeatures(rule.attribute.index.get)

      rule.predictedLabelAttribute(attributeValueToClassify)
    }

    dataset.withColumn($ { predictionCol }, classifyInstance(dataset($ {
      featuresCol
    })))
  }

  override def toString(): String =
    s"One Simple Rules Model is: \n ${rule.toString()}"

}

final class OneRClassifier(override val uid: String)
    extends Estimator[OneRClassifierModel]
    with OneRParams {

  def this() = this(Identifiable.randomUID("oneR"))

  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  // @Since("1.5.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  // @Since("1.5.0")

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setX(value: Double): this.type = set(reg, value)

  override def fit(dataset: Dataset[_]): OneRClassifierModel = {

    val schema = dataset.schema

    val attributesOption: Option[Array[Attribute]] =
      AttributeGroup.fromStructField(schema($(featuresCol))).attributes

    val dfToTrain: DataFrame = dataset.select($(featuresCol), $(labelCol))

    val attributes =
      attributesOption.getOrElse(default = throw new Exception("The attributes are missing"))

    val labelAttribte = Attribute.fromStructField(schema($(labelCol)))

    val numClasses = labelAttribte match {
      case binAttr: BinaryAttribute                  => Some(2)
      case nomAttr: NominalAttribute                 => nomAttr.getNumValues
      case _: NumericAttribute | UnresolvedAttribute => None
    }

    val dataToTrainRDD = dfToTrain.rdd // TODO: cache this RDD ???

    val output = attributes.map { attribute =>
      val partialDF = dataToTrainRDD.map {
        case Row(features: Vector, label: Double) =>
          val attributeValue = features.toArray(attribute.index.get)
          val counter        = Array.fill[Int](numClasses.get) { 0 }
          counter(label.toInt) += 1

          (attributeValue, counter) // The label is not necessary
      }

      val result = partialDF.reduceByKey {
        case (v1: Array[Int], v2: Array[Int]) =>
          v1.zip(v2).map { //TODO: Need Refactor!!
            case (x: Int, y: Int) => x + y
          }
      }

      val finalResult = result.map {
        case (attributeValue: Double, counter: Array[Int]) =>
          val score           = counter.max
          val indexOfMaxLabel = counter.indexOf(score)
          (attributeValue, indexOfMaxLabel.toDouble, score)
      }

      val mapOfRule = finalResult.map(x => (x._1, x._2)).collect().toMap

      val totalScore = finalResult.map(x => x._3).sum()

      OneRule(attribute, labelAttribte, mapOfRule, totalScore)

    }

    val betterRule: OneRule = output.maxBy(_.totalScore)

    val model = new OneRClassifierModel(uid, betterRule)

    copyValues(model)

  }

  override def copy(extra: ParamMap): Estimator[OneRClassifierModel] =
    defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    // TODO: check feature column
    require(!schema.fieldNames.contains(predictionCol), s"Prediction column already exists")
    StructType(schema.fields :+ StructField("prediction", DoubleType, false))
  }
}
