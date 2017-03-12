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

import java.text.SimpleDateFormat

import scala.collection.JavaConversions._

import org.apache.commons.csv._

import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{ BaseRelation, InsertableRelation, PrunedScan, TableScan }
import org.apache.spark.sql.types._

case class KeelRelation(
    baseRDD: () => RDD[String],
    location: Option[String],
    useHeader: Boolean = false,
    delimiter: Char = ',',
    quote: Character = '"',
    escape: Character = null,
    comment: Character = null,
    treatEmptyValuesAsNulls: Boolean,
    userSchema: StructType,
    codec: String = null,
    nullValue: String = "",
    dateFormat: String = null,
    maxCharsPerCol: Int = 100000
)(@transient val sqlContext: SQLContext)
    extends BaseRelation
    with TableScan
    with PrunedScan
    with InsertableRelation {

  val defaultCsvFormat =
    CSVFormat.DEFAULT.withRecordSeparator(System.getProperty("line.separator", "\n"))

  // Share date format object as it is expensive to parse date pattern.
  private val dateFormatter = if (dateFormat != null) new SimpleDateFormat(dateFormat) else null

  //  private val logger = LoggerFactory.getLogger(CsvRelation.getClass)

  override val schema: StructType = userSchema

  private def tokenRdd(header: Array[String]): RDD[Array[String]] = {

    val csvFormat = defaultCsvFormat
      .withDelimiter(delimiter)
      .withQuote(quote)
      .withEscape(escape)
      .withSkipHeaderRecord(false)
      .withHeader(header: _*)
      .withCommentMarker(comment)

    val filterLine = if (useHeader) firstLine else null

    baseRDD().mapPartitions { iter =>
      val csvIter = if (useHeader) {
        iter.filter(_ != filterLine)
      } else {
        iter
      }
      parseCSV(csvIter, csvFormat)
    }
  }

  override def buildScan: RDD[Row] = {
    val simpleDateFormatter = dateFormatter
    val schemaFields        = schema.fields
    val rowArray            = new Array[Any](schemaFields.length)
    tokenRdd(schemaFields.map(_.name)).flatMap { tokens =>
      var index: Int = 0
      try {
        index = 0
        while (index < schemaFields.length) {
          val field = schemaFields(index)
          rowArray(index) = TypeCast.castTo(tokens(index),
                                            field.dataType,
                                            field.nullable,
                                            treatEmptyValuesAsNulls,
                                            nullValue,
                                            simpleDateFormatter)
          index = index + 1
        }
        Some(Row.fromSeq(rowArray))
      } catch {
        case aiob: ArrayIndexOutOfBoundsException =>
          (index until schemaFields.length).foreach(ind => rowArray(ind) = null)
          Some(Row.fromSeq(rowArray))
      }
    }
  }

  /**
    * This supports to eliminate unneeded columns before producing an RDD
    * containing all of its tuples as Row objects. This reads all the tokens of each line
    * and then drop unneeded tokens without casting and type-checking by mapping
    * both the indices produced by `requiredColumns` and the ones of tokens.
    */
  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val simpleDateFormatter = dateFormatter
    val schemaFields        = schema.fields
    val requiredFields      = StructType(requiredColumns.map(schema(_))).fields
    val shouldTableScan     = schemaFields.deep == requiredFields.deep
    val safeRequiredFields  = requiredFields

    val rowArray = new Array[Any](safeRequiredFields.length)
    if (shouldTableScan) {
      buildScan()
    } else {
      val safeRequiredIndices = new Array[Int](safeRequiredFields.length)
      schemaFields.zipWithIndex.filter {
        case (field, _) => safeRequiredFields.contains(field)
      }.foreach {
        case (field, index) => safeRequiredIndices(safeRequiredFields.indexOf(field)) = index
      }
      val requiredSize = requiredFields.length
      tokenRdd(schemaFields.map(_.name)).flatMap { tokens =>
        val indexSafeTokens = if (schemaFields.length > tokens.length) {
          tokens ++ new Array[String](schemaFields.length - tokens.length)
        } else if (schemaFields.length < tokens.length) {
          tokens.take(schemaFields.length)
        } else {
          tokens
        }
        try {
          var index: Int    = 0
          var subIndex: Int = 0
          while (subIndex < safeRequiredIndices.length) {
            index = safeRequiredIndices(subIndex)
            val field = schemaFields(index)
            rowArray(subIndex) = TypeCast.castTo(indexSafeTokens(index),
                                                 field.dataType,
                                                 field.nullable,
                                                 treatEmptyValuesAsNulls,
                                                 nullValue,
                                                 simpleDateFormatter)
            subIndex = subIndex + 1
          }
          Some(Row.fromSeq(rowArray.take(requiredSize)))
        }
      }
    }
  }

  /**
    * Returns the first line of the first non-empty file in path
    */
  private lazy val firstLine = {
    if (comment != null) {
      baseRDD().filter { line =>
        line.trim.nonEmpty && !line.startsWith(comment.toString)
      }.first()
    } else {
      baseRDD().filter { line =>
        line.trim.nonEmpty
      }.first()
    }
  }

  private def parseCSV(iter: Iterator[String], csvFormat: CSVFormat): Iterator[Array[String]] =
    iter.flatMap { line =>
      try {
        val records = CSVParser.parse(line, csvFormat).getRecords
        if (records.isEmpty) {
          //          logger.warn(s"Ignoring empty line: $line")
          println(s"Ignoring empty line: $line")
          None
        } else {
          Some(records.head.toArray)
        }
      } catch {
        case scala.util.control.NonFatal(e) =>
          //          logger.error(s"Exception while parsing line: $line. ", e)
          println(s"Exception while parsing line: $line. ", e)
          None
      }
    }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = ???

}
