import sbt._
import sbtsparkpackage.SparkPackagePlugin.autoImport._

// format: off

object Version {
  final val Scala      = "2.11.8"
  final val ScalaTest  = "3.0.0"
  final val fastParser = "0.4.1"
}

object Library {
  val scalaTest = "org.scalatest" %% "scalatest" % Version.ScalaTest
  val fastParser = "com.lihaoyi" %% "fastparse" % Version.fastParser
  val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % "2.0.1_0.4.7"

}
