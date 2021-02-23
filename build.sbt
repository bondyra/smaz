name := "smaz"

version := "0.1"

scalaVersion := "2.12.7"

lazy val hello = (project in file("."))
  .settings(
    name := "smaz",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.0" % Compile,
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.1.0" % Compile,
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.0" % Compile
  )
