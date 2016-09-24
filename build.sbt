lazy val `datascience-models-sparkml` = project
  .in(file("."))
  .enablePlugins(AutomateHeaderPlugin, GitVersioning)

libraryDependencies ++= Vector(
  Library.scalaTest % "test"
)

initialCommands := """|import com.jmartinez.datascience.models.sparkml._
                      |""".stripMargin
