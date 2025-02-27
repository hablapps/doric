import scala.language.postfixOps

//import sbt.Compile

val stableVersion = "0.0.8"

val sparkDefaultShortVersion = "3.5"
val spark30Version           = "3.0.3"
val spark31Version           = "3.1.3"
val spark32Version           = "3.2.4"
val spark33Version           = "3.3.4"
val spark34Version           = "3.4.4"
val spark35Version           = "3.5.5"

val versionRegex      = """^(.*)\.(.*)\.(.*)$""".r
val versionRegexShort = """^(.*)\.(.*)$""".r

val scala212 = "2.12.20"
val scala213 = "2.13.14"

val parserSparkVersion: String => String = {
  case versionRegexShort("3", "0") => spark30Version
  case versionRegexShort("3", "1") => spark31Version
  case versionRegexShort("3", "2") => spark32Version
  case versionRegexShort("3", "3") => spark33Version
  case versionRegexShort("3", "4") => spark34Version
  case versionRegexShort("3", "5") => spark35Version
  case versionRegex("3", b, c)     => s"3.$b.$c"
}

val long2ShortVersion: String => String = { case versionRegex(a, b, _) =>
  s"$a.$b"
}

val scalaVersionSelect: String => List[String] = {
  case versionRegex("3", "0", _) => List(scala212)
  case versionRegex("3", "1", _) => List(scala212)
  case versionRegex("3", "2", _) => List(scala212, scala213)
  case versionRegex("3", "3", _) => List(scala212, scala213)
  case versionRegex("3", "4", _) => List(scala212, scala213)
  case versionRegex("3", "5", _) => List(scala212, scala213)
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
    name               := "doric_" + long2ShortVersion(sparkVersion.value),
    run / fork         := true,
    publish / skip     := false,
    publishArtifact    := true,
    scalaVersion       := scalaVersionSelect(sparkVersion.value).head,
    crossScalaVersions := scalaVersionSelect(sparkVersion.value),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided", // scala-steward:off
      "org.typelevel"          %% "cats-core"               % "2.12.0",
      "com.lihaoyi"            %% "sourcecode"              % "0.4.2",
      "com.chuusai"            %% "shapeless"               % "2.3.12",
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.12.0",
      "com.github.mrpowers"    %% "spark-fast-tests"        % "1.3.0"  % "test",
      "org.scalatest"          %% "scalatest"               % "3.2.19" % "test"
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
      "SPARK_VERSION"  -> sparkVersion.value,
      "SPARK_SHORT_VERSION" -> long2ShortVersion(sparkVersion.value)
        .replace(".", "-"),
      "SCALA_SHORT_VERSION" -> long2ShortVersion(scalaVersion.value)
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
  .enablePlugins(plugins *)

// Scoverage settings
Global / coverageEnabled       := false
Global / coverageFailOnMinimum := false
Global / coverageHighlighting  := true
