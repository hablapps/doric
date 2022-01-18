import sbt.Compile

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
  ),
  Developer(
    "AlfonsoRR",
    "Eduardo Ruiz",
    "",
    url("https://github.com/eruizalo")
  )
)

Global / scalaVersion := "2.12.15"

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

val sparkVersion = "3.1.2"
lazy val core = project
  .in(file("core"))
  .settings(
    name       := "doric",
    run / fork := true,
    libraryDependencies ++= Seq(
      "org.apache.spark"    %% "spark-sql"        % sparkVersion % "provided",
      "org.typelevel"       %% "cats-core"        % "2.7.0",
      "com.lihaoyi"         %% "sourcecode"       % "0.2.7",
      "io.monix"            %% "newtypes-core"    % "0.2.1",
      "com.github.mrpowers" %% "spark-daria"      % "1.0.0"      % "test",
      "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0"      % "test",
      "org.scalatest"       %% "scalatest"        % "3.2.10"     % "test"
    ),
    // docs
    run / fork                      := true,
    Compile / doc / autoAPIMappings := true,
    Compile / doc / scalacOptions ++= Seq(
      "-groups",
      "-implicits",
      "-skip-packages",
      "org.apache.spark"
    )
  )

lazy val docs = project
  .in(file("docs"))
  .dependsOn(core)
  .settings(
    run / fork := true,
    run / javaOptions += "-XX:MaxJavaStackTraceDepth=10",
    mdocIn := baseDirectory.value / "docs",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion
    ),
    mdocVariables := Map(
      "VERSION"        -> version.value,
      "STABLE_VERSION" -> "0.0.1",
      "SPARK_VERSION"  -> sparkVersion
    ),
    mdocExtraArguments := Seq(
      "--clean-target"
    )
  )
  .enablePlugins(MdocPlugin)

Global / scalacOptions ++= Seq(
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

// Scoverage settings
Global / coverageEnabled       := false
Global / coverageFailOnMinimum := false
Global / coverageHighlighting  := true
