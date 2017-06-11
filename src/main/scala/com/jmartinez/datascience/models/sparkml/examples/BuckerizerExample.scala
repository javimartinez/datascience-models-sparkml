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

package com.jmartinez.datascience.models.sparkml.examples

import org.apache.spark.sql.SparkSession


  object BuckerizerExample {

  def main(args: Array[String]): Unit = {

    val spark =
      SparkSession.builder.appName("WangMendelAlgorithm")
        .master("local[4]")
        .getOrCreate()

//    println("Atr-0 " + "Array(" + getBucket(0.0,58329.0).mkString(",") + ")")
//    println("Atr-0 " + "Array(" + getBucket(0.0,58329.0).mkString(",") + ")")
//    println("Atr-4 " + "Array(" + getBucket(0.0, 693375640.0).mkString(",") + ")")
//    println("Atr-5 " + "Array(" + getBucket(0.0, 5155468.0).mkString(",") + ")")
//    println("Atr-9 " + "Array(" + getBucket(0.0, 30.0).mkString(",") + ")")
//    println("Atr-12 " + "Array(" + getBucket(0.0, 884.0).mkString(",") + ")")
//    println("Atr-15 " + "Array(" + getBucket(0.0, 993.0).mkString(",") + ")")
//    println("Atr-16 " + "Array(" + getBucket(0.0, 28.0).mkString(",") + ")")
//    println("Atr-12 " + "Array(" + getBucket(0.0, 884.0).mkString(",") + ")")
//    println("Atr-15 " + "Array(" + getBucket(0.0, 993.0).mkString(",") + ")")
//    println("Atr-16 " + "Array(" + getBucket(0.0, 28.0).mkString(",") + ")")
//    println("Atr-22 " + "Array(" + getBucket(0.0, 511.0).mkString(",") + ")")
//    println("Atr-23 " + "Array(" + getBucket(0.0, 511.0).mkString(",") + ")")
//    println("Atr-31 " + "Array(" + getBucket(0.0, 255.0).mkString(",") + ")")
//    println("Atr-32 " + "Array(" + getBucket(0.0, 255.0).mkString(",") + ")")

    println("FLength  " + "Array(" + getBucket(4.2835, 334.177).mkString(",") + ")")
    println("FWidth  " + "Array(" + getBucket(0.0, 256.382).mkString(",") + ")")
    println("FSize  " + "Array(" + getBucket(1.9413, 5.3233).mkString(",") + ")")
    println("FConc  " + "Array(" + getBucket(0.0131, 0.893).mkString(",") + ")")
    println("FConc1  " + "Array(" + getBucket(3.0E-4, 0.6752).mkString(",") + ")")
    println("FAsym  " + "Array(" + getBucket(-457.9161, 575.2407).mkString(",") + ")")
    println("FM3Long  " + "Array(" + getBucket(-331.78, 238.321).mkString(",") + ")")
    println("FM3Trans  " + "Array(" + getBucket(-205.8947, 179.851).mkString(",") + ")")
    println("FAlpha  " + "Array(" + getBucket(0.0, 90.0).mkString(",") + ")")
    println("FDist  " + "Array(" + getBucket(1.2826, 495.561).mkString(",") + ")")


    val splits = Array(Double.NegativeInfinity, 0.0, 1.0,Double.PositiveInfinity)

////    val data = Array(-999.9, -0.5, -0.3, 0.0, 0.2, 999.9)
//    val data = Array(0.0,1.0,1.0)
//    val dataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
//
//    val bucketizer = new Bucketizer()
//      .setInputCol("features")
//      .setOutputCol("bucketedFeatures")
//      .setSplits(splits)
//
//    // Transform original data into its bucket index.
//    val bucketedData = bucketizer.transform(dataFrame)
//
//    bucketedData.show()

    spark.stop()

  }

    def getBucket(min:Double, max:Double): Array[Double] = {

      val amplitude = (max - min) / 6 // default
      val array = new Array[Double](7)
      var a = min
      var c = 0
      while (a <= max) {
        array(c) = a
        a+=amplitude
        c+=1
      }
      array
    }
}
