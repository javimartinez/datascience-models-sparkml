import Build._

lazy val `datascience-models-sparkml` = project
  .in(file("."))
  .enablePlugins(AutomateHeaderPlugin, GitVersioning)


version := "0.0.1-SNAPSHOT"

sparkVersion := "2.0.0"

sparkComponents ++= Seq("core", "sql", "mllib")

spDependencies += "com.databricks/spark-csv_2.11:1.5.0"

parallelExecution in Test := false

//fork := true

libraryDependencies ++= Vector(
  Library.scalaTest % "test",
  Library.fastParser,
  Library.sparkTestingBase
)

initialCommands := """|import com.jmartinez.datascience.models.sparkml._
                      |""".stripMargin

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.startsWith("META-INF") => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", xs @ _*) => MergeStrategy.first
  case PathList("org", "jboss", xs @ _*) => MergeStrategy.first
  case "log4j.properties" => MergeStrategy.discard
  case "about.html"  => MergeStrategy.rename
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}
}
