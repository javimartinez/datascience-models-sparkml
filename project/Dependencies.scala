import sbt._

// format: off

object Version {
  final val Scala = "2.11.8"
  final val ScalaTest = "3.0.0"
  final val fastParser = "0.4.1"
  final val SparkVersion = "2.1.0"
  final val SparkCsvVersion = "1.5.0"
}

object Library {

  import Version._

  val ScalaTest = "org.scalatest" %% "scalatest" % Version.ScalaTest
  val FastParser = "com.lihaoyi" %% "fastparse" % Version.fastParser
  val SparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % "2.0.1_0.4.7"

  val SparkCore = "org.apache.spark" %% "spark-core" % SparkVersion
  val SparkSql = "org.apache.spark" %% "spark-sql" % SparkVersion
  val SparkMllib = "org.apache.spark" %% "spark-mllib" % SparkVersion

//  val SparkCore = "org.apache.spark" %% "spark-core" % SparkVersion % "provided"
//  val SparkSql = "org.apache.spark" %% "spark-sql" % SparkVersion % "provided"
//  val SparkMllib = "org.apache.spark" %% "spark-mllib" % SparkVersion % "provided"

  val SparkCsv = "com.databricks" %% "spark-csv" % SparkCsvVersion
}
