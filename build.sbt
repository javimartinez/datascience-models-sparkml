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
  Library.scalaTest % "test"
)

initialCommands := """|import com.jmartinez.datascience.models.sparkml._
                      |""".stripMargin
