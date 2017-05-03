import Build._

lazy val `datascience-models-sparkml` = project
  .in(file("."))
  .enablePlugins(AutomateHeaderPlugin, GitVersioning)

version := "0.0.1-SNAPSHOT"

parallelExecution in Test := false

//fork := true

libraryDependencies ++= Vector(
  Library.ScalaTest % "test",
  Library.FastParser,
  Library.SparkTestingBase,
  Library.SparkCore,
  Library.SparkSql,
  Library.SparkMllib,
  Library.SparkCsv
)

initialCommands := """|import com.jmartinez.datascience.models.sparkml._
                      |""".stripMargin


assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _ *)        => MergeStrategy.last
  case PathList("javax", "activation", xs @ _ *)     => MergeStrategy.last
  case PathList("org", "apache", xs @ _ *)           => MergeStrategy.last
  case PathList("com", "google", xs @ _ *)           => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _ *) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _ *)         => MergeStrategy.last
  case PathList("com", "yammer", xs @ _ *)           => MergeStrategy.last
  case "about.html"                                  => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA"                       => MergeStrategy.last
  case "META-INF/mailcap"                            => MergeStrategy.last
  case "META-INF/mimetypes.default"                  => MergeStrategy.last
  case "plugin.properties"                           => MergeStrategy.last
  case "log4j.properties"                            => MergeStrategy.last
  case m if m.toLowerCase.endsWith("manifest.mf")    => MergeStrategy.discard
  case m if m.startsWith("META-INF")                 => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _ *)        => MergeStrategy.first
  case PathList("org", "apache", xs @ _ *)           => MergeStrategy.first
  case PathList("org", "jboss", xs @ _ *)            => MergeStrategy.first
  case "log4j.properties"                            => MergeStrategy.discard
  case "about.html"                                  => MergeStrategy.rename
  case "reference.conf"                              => MergeStrategy.concat
  case _                                             => MergeStrategy.first
}
