

resolvers += "Spark Package Main Repo" at "https://dl.bintray.com/spark-packages/maven"


addSbtPlugin("com.geirsson"      % "sbt-scalafmt" % "0.3.1")
addSbtPlugin("com.typesafe.sbt"  % "sbt-git"      % "0.8.5")
addSbtPlugin("de.heikoseeberger" % "sbt-header"   % "1.6.0")
addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.2")
