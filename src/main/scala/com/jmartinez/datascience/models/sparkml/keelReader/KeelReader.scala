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

import com.jmartinez.datascience.models.sparkml.keelReader.HeaderParser._
import fastparse.noApi._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import AttributeConverters._


object KeelReader {

  implicit class KeelReader(spark: SparkSession) {

    def keelFile(filePath: String): DataFrame = {

      val rdd: RDD[String] =
        spark.sparkContext.textFile(filePath)

      val (headerLines, csvLines) = divideKeelRDD(rdd)

      val schema = readHeader(headerLines)

      val csvRelation = KeelRelation(() => csvLines,
        location = Some(filePath),
        userSchema = schema,
        treatEmptyValuesAsNulls = false)(spark.sqlContext)

      spark.sqlContext.baseRelationToDataFrame(csvRelation)

    }
  }

  private def readHeader(headerLines: List[String]): StructType = {

    val attributesLines =
      headerLines.filter(_.contains("@attribute")) //TODO: only attributes for now

    val structFields =
      attributesLines.map(parseKeelAttributeLine).map(structFieldFromKeelAttribute)

    StructType(structFields)

  }

  private def parseKeelAttributeLine(line: String): KeelAttribute = {
    keelHeaderParser.parse(line) match {
      case Parsed.Success(keelAttribute, _) =>
        keelAttribute
      case Parsed.Failure(_, _, extraFailure) =>
        throw new Exception(extraFailure.traced.trace)
    }
  }

  private def structFieldFromKeelAttribute(keelAttribute: KeelAttribute): StructField = {

    keelAttribute match { // TODO: nullable harcoded
      case att: NumericAttribute =>
        StructField(att.name, DoubleType, false, att.toMetadata())
      case att: CategoricalAttribute =>
        StructField(att.name, StringType, false, Metadata.empty)
    }
  }

  private def divideKeelRDD(inputRDD: RDD[String]): (List[String], RDD[String]) = {
    //TODO: workaround: parse header lines in CsvRelation (KeelRelation)

    val headerLines: List[String] =
      inputRDD.filter(line => line.startsWith("@")).collect().toList
    val csvLines: RDD[String] = inputRDD.filter(line => !line.startsWith("@"))

    (headerLines, csvLines)
  }

}
