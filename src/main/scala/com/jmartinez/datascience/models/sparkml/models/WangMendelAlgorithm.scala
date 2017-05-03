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

import com.jmartinez.datascience.models.sparkml.Fuzzy.{
  FuzzyPartition,
  FuzzyRegionSingleton,
  FuzzyRule
}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml._
import org.apache.spark.ml.attribute.{
  Attribute,
  AttributeGroup,
  NominalAttribute,
  NumericAttribute
}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ DoubleType, StructField, StructType }
import org.apache.spark.sql.{ Dataset, Row }
import org.apache.spark.storage.StorageLevel

object WangMendelUtils {

  def evaluateMembershipAntecedents(
      features: Array[Double],
      indexOfFuzzyRegionRule: Array[Int],
      fuzzyPartitions: Array[FuzzyPartition]
  ): Double = {
    val degrees: Array[Double] = features.zipWithIndex.map {
      case (feaureValue, featureIndex) =>
        fuzzyPartitions(featureIndex)
          .regions(indexOfFuzzyRegionRule(featureIndex))
          .membershipOf(feaureValue)
    }

    tnorm(degrees)
  }

  // hacer una funcion para unir el degree de los antecedentes con el degree del consequente.
  // La funcion de membresia puede ser otra que no sea triangular -> eso implica mucho cambio, merece la pena??
  // la funcion de create Fuzzy partition tambien puede cambiar !! OOOMYGoooddd

  def tnorm(degrees: Array[Double]): Double = degrees.product

  // for now
  def tnorm(x: Double, y: Double): Double = x * y

  // deffuzzy method Weighted average method

  /**
    * Not tested yet
    */
  def getMaxAndMinOfAllFeatures(
      dataset: RDD[Row],
      attributes: Array[Attribute]
  ): Array[(Int, (Double, Double))] = {

    val attributesIndex = attributes.flatMap(_.index) // no me asegura que esten todos los atributos BroadCast??

    dataset.flatMap {
      case (Row(features: Vector, _)) => //vectorUDT
        attributesIndex.map(idx => (idx, (features(idx), features(idx))))
    }.reduceByKey {
      case ((elemToMax1, elemToMin1), (elemToMax2, elemToMin2)) =>
        (scala.math.max(elemToMax1, elemToMax2), scala.math.min(elemToMin1, elemToMin2))
    }.collect
  }
}

final class WangMendelModel(
    override val uid: String,
    val ruleBase: Array[FuzzyRule],
    val fuzzyPartitionsFeatures: Array[FuzzyPartition],
    val fuzzyPartitionsLabel: FuzzyPartition
) extends PredictionModel[DenseVector, WangMendelModel] {

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    // TODO: check feature column
    require(!schema.fieldNames.contains(predictionCol), s"Prediction column already exists")
    StructType(schema.fields :+ StructField("prediction", DoubleType, false))
  }

  override def copy(extra: ParamMap): WangMendelModel = {
    val copied = new WangMendelModel(uid, ruleBase, fuzzyPartitionsFeatures, fuzzyPartitionsLabel)
    copyValues(copied, extra).setParent(parent)
  }

  override protected def predict(features: DenseVector): Double = {

    val arrayAcc = Array.fill[Double](10) { 0.0 }

    val consequentIndex = {
      ruleBase.foreach { rule =>
        arrayAcc.update(rule.consequent,
          arrayAcc(rule.consequent) + WangMendelUtils.evaluateMembershipAntecedents(
            features.toArray,
            rule.antecedents,
            fuzzyPartitionsFeatures
          ))
      }
      arrayAcc.zipWithIndex.maxBy(_._1)._2
    }

    fuzzyPartitionsLabel.regions(consequentIndex) match {
      case fr: FuzzyRegionSingleton => fr.center
      case _                        => 0
    }
  }
}

final class WangMendelAlgorithm(override val uid: String)
    extends Predictor[DenseVector, WangMendelAlgorithm, WangMendelModel]
    with WangMendelParams {

  def this() = this(Identifiable.randomUID("Wang&Mendel"))

  /** @group setParam */
  def setNumFuzzyRegions(value: Int): this.type = set(numFuzzyRegions, value)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    // TODO: check feature column
    require(!schema.fieldNames.contains(predictionCol), s"Prediction column already exists")
    StructType(schema.fields :+ StructField("prediction", DoubleType, false))
  }

  override def copy(extra: ParamMap): WangMendelAlgorithm = defaultCopy(extra)

  override protected def train(dataset: Dataset[_]): WangMendelModel = {

    val datasetRdd = dataset.select($(featuresCol), $(labelCol)).rdd.map {
      case (Row(features: DenseVector, label: Double)) =>
        (features.toArray, label)
    }

//    datasetRdd.persist(StorageLevel.MEMORY_AND_DISK)

    // Step 1: Divide de Input and Output Spaces into Fuzzy Regions

    val features: Array[Attribute] =
      AttributeGroup
        .fromStructField(dataset.schema($(featuresCol)))
        .attributes
        .getOrElse(throw new Exception("The attributes are missing"))

    val fuzzyPartitionsOfFeatures: Array[FuzzyPartition] = features.map {
      case att: NumericAttribute =>
        FuzzyPartition(att.min.get, att.max.get, $(numFuzzyRegions)) // create fuzzy partition
      case _ => throw new Exception("Not supported")
    }

    val fuzzyPartitionsOfLabel: FuzzyPartition =
      Attribute.fromStructField(dataset.schema($(labelCol))) match {
        // TODO: workaround from keel algorithm, I don't know why do this (Generating One Single partition for each value of output attribute)
        case att: NumericAttribute =>
          FuzzyPartition(att.max.get.toInt)
        case att: NominalAttribute =>
          FuzzyPartition(att.values.getOrElse(throw new Exception("Not supported")).length)
        case _ =>
          throw new Exception("Not supported")
      }

    // MEJOR SE PUEDE UNIR EN UN SOLO ARRAY[FUZZYPARTITION] Y CON EL INDEX SABER SI ES DE FEATURES O LABEL

    // Step 2: Generate Fuzzy Rules from given Data Pairs and
    // Step 3: Assign degree for each rule
    // Step 4: Create a Combined Fuzzy Rule Base

    val ruleBase = datasetRdd.map {
      case (features: Array[Double], label: Double) =>
        generateFuzzyRule(features, label, fuzzyPartitionsOfFeatures, fuzzyPartitionsOfLabel)
    }.map(fuzzyRule => (fuzzyRule.getLabelsOfAntecedentsCodified, fuzzyRule))
      .reduceByKey {
        // in conflict rules choose the rule with maximum degree
        case (fuzzyRule1: FuzzyRule, fuzzyRule2: FuzzyRule) =>
          if (fuzzyRule1.degree >= fuzzyRule2.degree)
            fuzzyRule1
          else fuzzyRule2
      }
      .values
      .collect()

    println(s"The size of rule base is: ${ruleBase.length}")
    println(s"rule baseeee")

    val a = ruleBase.map(r => r.consequent)
    a.distinct.foreach(println)

    new WangMendelModel(uid, ruleBase, fuzzyPartitionsOfFeatures, fuzzyPartitionsOfLabel)
  }

  def generateFuzzyRule(
      features: Array[Double],
      label: Double,
      fuzzyPartitionsOfFeatures: Array[FuzzyPartition],
      fuzzyPartitionsOfLabel: FuzzyPartition
  ): FuzzyRule = {

    // find the better fuzzy partition for each feature
    val indexsOfFuzzyRegionsOfFeatures = features.zipWithIndex.map {
      case (featureValue: Double, featureIndex: Int) =>
        fuzzyPartitionsOfFeatures(featureIndex).findFuzzyRegionWithMaxDegreeFor(featureValue)
    }

    val indexOfFuzzyRegionOfLabel =
      fuzzyPartitionsOfLabel.findFuzzyRegionWithMaxDegreeFor(label)

    val degreeOfRule =
      WangMendelUtils.tnorm(
        WangMendelUtils.evaluateMembershipAntecedents(
          features,
          indexsOfFuzzyRegionsOfFeatures,
          fuzzyPartitionsOfFeatures
        ),
        fuzzyPartitionsOfLabel.regions(indexOfFuzzyRegionOfLabel).membershipOf(label)
      )

    FuzzyRule(indexsOfFuzzyRegionsOfFeatures, indexOfFuzzyRegionOfLabel, degreeOfRule)
  }

}
