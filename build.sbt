ThisBuild / organization := "org.hablapps"
ThisBuild / homepage     := Some(url("https://github.com/hablapps/doric"))
ThisBuild / licenses := List(
  "Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0")
)
ThisBuild / developers := List(
  Developer(
    "AlfonsoRR",
    "Alfonso Roa",
    "@saco_pepe",
    url("https://github.com/alfonsorr")
  )
)

name := "doric"

scalaVersion := "2.12.14"

libraryDependencies ++= Seq(
  "org.apache.spark"    %% "spark-sql"        % "3.1.2" % "provided",
  "org.typelevel"       %% "cats-core"        % "2.6.1",
  "com.lihaoyi"         %% "sourcecode"       % "0.2.7",
  "com.github.mrpowers" %% "spark-daria"      % "1.0.0" % "test",
  "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0" % "test",
  "org.scalatest"       %% "scalatest"        % "3.2.9" % "test"
)

// scaladoc settings
Compile / doc / scalacOptions ++= Seq("-groups")

// test suite settings
Test / fork := true
javaOptions ++= Seq(
  "-Xms512M",
  "-Xmx2048M",
  "-XX:MaxPermSize=2048M",
  "-XX:+CMSClassUnloadingEnabled"
)
// Show runtime of tests
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

scmInfo := Some(
  ScmInfo(
    url("https://github.com/hablapps/doric"),
    "git@github.com:hablapps/doric.git"
  )
)

updateOptions := updateOptions.value.withLatestSnapshots(false)

scalacOptions ++= Seq(
  "-encoding",
  "utf8",             // Option and arguments on same line
  "-Xfatal-warnings", // New lines for each options
  "-deprecation",
  "-unchecked",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:existentials",
  "-language:postfixOps",
  "-Ypartial-unification",
  "-Ywarn-numeric-widen"
)
