package com.jmartinez.datascience.models.sparkml.examples

import com.jmartinez.datascience.models.sparkml.models.OneRClassifier
import org.apache.log4j.{Level, Logger}

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NominalAttribute}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.SparkSession


object OneRClassifierExample {

  def main(args: Array[String]) {

    // Disable INFO Log
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("OneRClassifierExample")
      .getOrCreate()

    // Creates a DataFrame

    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("/Users/Javi/Development/data/weatherDataSet.csv")


    df.show()

    println(s"The dataTypes are: ${df.dtypes.foreach(tuple => println(tuple))}")

    // Transformer

    val indexer1 = new StringIndexer()
      .setInputCol("outlook")
      .setOutputCol("idx_outlook")

    val indexer2 = new StringIndexer()
      .setInputCol("temperature")
      .setOutputCol("idx_temperature")

    val indexer3 = new StringIndexer()
      .setInputCol("humidity")
      .setOutputCol("idx_humidity")

    val indexer4 = new StringIndexer()
      .setInputCol("play")
      .setOutputCol("idx_play")

    val indexerPipeline = new Pipeline()
      .setStages(Array(indexer1, indexer2, indexer3, indexer4))

    val indexedDF = indexerPipeline.fit(df).transform(df)

    indexedDF.show()

    val attributeValues = Attribute
      .fromStructField(indexedDF.schema("idx_outlook")).asInstanceOf[NominalAttribute].values.get

    println("!!!!! Los valores del attributo idx_outlook son:     ")

    attributeValues.foreach(println)


    val assembler = new VectorAssembler()
      .setInputCols(Array("idx_outlook", "idx_temperature",
        "idx_humidity", "windy"))
      .setOutputCol("features")

    val assemblerPL = new Pipeline()
      .setStages(Array(assembler))

    val assemblerDF = assemblerPL.fit(indexedDF).transform(indexedDF)


    assemblerDF.show()


    val attributeValues2 = AttributeGroup.fromStructField(assemblerDF.schema("features")).attributes

    attributeValues2.get(0).asInstanceOf[NominalAttribute].values.get.foreach(println)
    println("!!!!! Los valores del attributo idx_outlook son:     ")

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.

    val labelIndexer = new StringIndexer()
      .setInputCol(df.columns.last)
      .setOutputCol("label")

    // Automatically identify categorical features, and index them.

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(5) // features with > 4 distinct values are treated as continuous

    val prepareDataPipeline = new Pipeline()
      .setStages(Array(featureIndexer))

    val preTrainDF = prepareDataPipeline.fit(assemblerDF).transform(assemblerDF)

    preTrainDF.show()

    val dataFrameToTrain = preTrainDF.select("indexedFeatures", "idx_play")
    dataFrameToTrain.show()

    val oneR = new OneRClassifier()
      .setLabelCol("idx_play")
      .setFeaturesCol("indexedFeatures")
      .setPredictionCol("predicedLabel")

    oneR.fit(dataFrameToTrain)

    spark.stop()
  }

}
