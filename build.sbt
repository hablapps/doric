import scala.language.postfixOps

import sbt.Compile

val stableVersion = "0.0.5"

val sparkDefaultShortVersion = "3.3"
val spark24Version           = "2.4.8"
val spark30Version           = "3.0.3"
val spark31Version           = "3.1.3"
val spark32Version           = "3.2.2"
val spark33Version           = "3.3.0"

val versionRegex      = """^(.*)\.(.*)\.(.*)$""".r
val versionRegexShort = """^(.*)\.(.*)$""".r

val scala211 = "2.11.12"
val scala212 = "2.12.15"
val scala213 = "2.13.8"

val parserSparkVersion: String => String = {
  case versionRegexShort("2", "4") => spark24Version
  case versionRegexShort("3", "0") => spark30Version
  case versionRegexShort("3", "1") => spark31Version
  case versionRegexShort("3", "2") => spark32Version
  case versionRegexShort("3", "3") => spark33Version
  case versionRegex(a, b, c)       => s"$a.$b.$c"
}

val sparkLong2ShortVersion: String => String = { case versionRegex(a, b, _) =>
  s"$a.$b"
}

val scalaVersionSelect: String => List[String] = {
  case versionRegex("2", _, _)   => List(scala211)
  case versionRegex("3", "0", _) => List(scala212)
  case versionRegex("3", "1", _) => List(scala212)
  case versionRegex("3", "2", _) => List(scala212, scala213)
  case versionRegex("3", "3", _) => List(scala212, scala213)

}

val catsVersion: String => String = {
  case versionRegex("2", _, _) => "2.0.0"
  case _                       => "2.7.0"
}

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
    "eruizalo",
    "Eduardo Ruiz",
    "",
    url("https://github.com/eruizalo")
  )
)
val sparkVersion = settingKey[String]("Spark version")
Global / sparkVersion :=
  parserSparkVersion(
    System.getProperty("sparkVersion", sparkDefaultShortVersion)
  )
Global / scalaVersion    := scalaVersionSelect(sparkVersion.value).head
Global / publish / skip  := true
Global / publishArtifact := false

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

val configSpark = Seq(
  sparkVersion := parserSparkVersion(
    System.getProperty("sparkVersion", sparkDefaultShortVersion)
  )
)

val scalaOptionsCommon = Seq(
  "-encoding",
  "utf8",             // Option and arguments on same line
  "-Xfatal-warnings", // New lines for each options
  "-deprecation",
  "-unchecked",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:existentials",
  "-language:postfixOps",
  "-Ywarn-numeric-widen"
)
lazy val core = project
  .in(file("core"))
  .settings(
    configSpark,
    name               := "doric_" + sparkLong2ShortVersion(sparkVersion.value),
    run / fork         := true,
    publish / skip     := false,
    publishArtifact    := true,
    scalaVersion       := scalaVersionSelect(sparkVersion.value).head,
    crossScalaVersions := scalaVersionSelect(sparkVersion.value),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided", // scala-steward:off
      "org.typelevel" %% "cats-core"  % catsVersion(sparkVersion.value),
      "com.lihaoyi"   %% "sourcecode" % "0.3.0",
      "com.chuusai"   %% "shapeless"  % "2.3.9",
      "com.github.mrpowers" %% "spark-fast-tests" % "1.3.0"  % "test",
      "org.scalatest"       %% "scalatest"        % "3.2.13" % "test"
    ),
    // docs
    run / fork                      := true,
    Compile / doc / autoAPIMappings := true,
    Compile / doc / scalacOptions ++= Seq(
      "-groups",
      "-implicits",
      "-skip-packages",
      "org.apache.spark"
    ),
    Compile / unmanagedSourceDirectories ++= {
      (sparkVersion.value match {
        case versionRegex(mayor, minor, _) =>
          (Compile / sourceDirectory).value ** s"*spark_*$mayor.$minor*" / "scala" get
      }) ++
        (scalaVersion.value match {
          case versionRegex(mayor, minor, _) =>
            (Compile / sourceDirectory).value ** s"*scala_*$mayor.$minor*" / "scala" get
        })
    },
    Test / unmanagedSourceDirectories ++= {
      (sparkVersion.value match {
        case versionRegex(mayor, minor, _) =>
          (Test / sourceDirectory).value ** s"*spark_*$mayor.$minor*" / "scala" get
      }) ++
        (scalaVersion.value match {
          case versionRegex(mayor, minor, _) =>
            (Test / sourceDirectory).value ** s"*scala_*$mayor.$minor*" / "scala" get
        })
    },
    scalacOptions ++= {
      scalaOptionsCommon ++ {
        if (scalaVersion.value.startsWith("2.13"))
          Seq.empty
        else
          Seq("-Ypartial-unification")
      }
    }
  )

val plugins = parserSparkVersion(
  System.getProperty("sparkVersion", sparkDefaultShortVersion)
) match {
  case versionRegex("2", "4", _) => List.empty[Plugins]
  case _                         => List(MdocPlugin)
}

lazy val docs = project
  .in(file("docs"))
  .dependsOn(core)
  .settings(
    configSpark,
    run / fork      := true,
    publish / skip  := true,
    publishArtifact := false,
    run / javaOptions += "-XX:MaxJavaStackTraceDepth=10",
    scalaVersion := scalaVersionSelect(sparkVersion.value).head,
    mdocIn       := baseDirectory.value / "docs",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion.value
    ),
    mdocVariables := Map(
      "VERSION"        -> version.value,
      "STABLE_VERSION" -> stableVersion,
      "SPARK_VERSION"  -> sparkVersion.value
    ),
    mdocExtraArguments := Seq(
      "--clean-target"
    ),
    scalacOptions ++= {
      scalaOptionsCommon ++ {
        if (scalaVersion.value.startsWith("2.13"))
          Seq.empty
        else
          Seq("-Ypartial-unification")
      }
    }
  )
  .enablePlugins(plugins: _*)

// Scoverage settings
Global / coverageEnabled       := false
Global / coverageFailOnMinimum := false
Global / coverageHighlighting  := true
